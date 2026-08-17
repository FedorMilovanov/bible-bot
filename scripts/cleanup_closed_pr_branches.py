#!/usr/bin/env python3
"""Delete stale service-owned work branches without losing unique history.

Automatic cleanup uses reproducible GitHub facts first:

1. the current ref SHA exactly matches the head of a same-repository PR that was
   merged into the current default branch, and that PR's merge commit is still
   reachable from the captured default-branch SHA;
2. GitHub proves the current ref SHA is already an ancestor of the captured
   default-branch SHA;
3. every branch-side changed path is byte-for-byte represented in the captured
   default-branch tree.

A narrow reviewed-retirement manifest may authorize deletion only after those
generic proofs fail. Its entries pin an exact branch tip and a replacement
commit that must still be reachable from the captured default branch. Open PR
heads and bases always block cleanup. Historical audit archives remain
non-authoritative and are deliberately not read by this script.

Only ``agent/``, ``release/``, ``dependabot/``, ``hardening/``, ``retire/`` and
``audit/`` refs are eligible. Every candidate ref is re-read immediately before
DELETE, and default-branch-dependent proofs are invalidated if main moves. The
script never force-updates a ref.
"""
from __future__ import annotations

import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

API_ROOT = "https://api.github.com"
API_VERSION = "2022-11-28"
ELIGIBLE_PREFIXES = (
    "agent/",
    "release/",
    "dependabot/",
    "hardening/",
    "retire/",
    "audit/",
)
PAGE_SIZE = 100
_MAX_COMPARE_FILES = 300
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
RETIREMENT_MANIFEST_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "reviewed-branch-retirements.json"
)


def _eligible_ref(ref: str, *, default_branch: str) -> bool:
    return bool(ref) and ref != default_branch and ref.startswith(ELIGIBLE_PREFIXES)


def _merged_default_head_shas(
    closed_pulls: list[dict],
    *,
    repository: str,
    default_branch: str,
) -> dict[str, set[str]]:
    """Return exact heads of PRs proven merged into this repo's default branch."""
    result: dict[str, set[str]] = defaultdict(set)
    for pull in closed_pulls:
        if not pull.get("merged_at"):
            continue

        head = pull.get("head") or {}
        head_repo = head.get("repo") or {}
        base = pull.get("base") or {}
        base_repo = base.get("repo") or {}
        ref = str(head.get("ref") or "")
        sha = str(head.get("sha") or "")

        if head_repo.get("full_name") != repository:
            continue
        if base_repo.get("full_name") != repository:
            continue
        if base.get("ref") != default_branch:
            continue
        if not _eligible_ref(ref, default_branch=default_branch) or not sha:
            continue
        result[ref].add(sha)
    return dict(result)


def _merged_default_merge_shas(
    closed_pulls: list[dict],
    *,
    repository: str,
    default_branch: str,
) -> dict[tuple[str, str], set[str]]:
    """Map exact merged PR heads to merge commits that must remain in current main."""
    result: dict[tuple[str, str], set[str]] = defaultdict(set)
    for pull in closed_pulls:
        if not pull.get("merged_at"):
            continue

        head = pull.get("head") or {}
        head_repo = head.get("repo") or {}
        base = pull.get("base") or {}
        base_repo = base.get("repo") or {}
        ref = str(head.get("ref") or "")
        head_sha = str(head.get("sha") or "")
        merge_sha = str(pull.get("merge_commit_sha") or "")

        if head_repo.get("full_name") != repository:
            continue
        if base_repo.get("full_name") != repository:
            continue
        if base.get("ref") != default_branch:
            continue
        if not _eligible_ref(ref, default_branch=default_branch):
            continue
        if not head_sha or not merge_sha:
            continue
        result[(ref, head_sha)].add(merge_sha)
    return dict(result)


def _open_head_refs(open_pulls: list[dict], *, repository: str) -> set[str]:
    refs: set[str] = set()
    for pull in open_pulls:
        head = pull.get("head") or {}
        head_repo = head.get("repo") or {}
        ref = str(head.get("ref") or "")
        if head_repo.get("full_name") == repository and ref:
            refs.add(ref)
    return refs


def _open_base_refs(open_pulls: list[dict], *, repository: str) -> set[str]:
    refs: set[str] = set()
    for pull in open_pulls:
        base = pull.get("base") or {}
        base_repo = base.get("repo") or {}
        ref = str(base.get("ref") or "")
        if base_repo.get("full_name") == repository and ref:
            refs.add(ref)
    return refs


def _load_reviewed_retirements(
    path: Path = RETIREMENT_MANIFEST_PATH,
) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or raw.get("schema_version") != 1:
        raise ValueError("reviewed retirement manifest schema is invalid")

    entries = raw.get("retirements")
    if not isinstance(entries, list):
        raise ValueError("reviewed retirement manifest entries are invalid")

    result: dict[str, dict[str, str]] = {}
    for item in entries:
        if not isinstance(item, dict):
            raise ValueError("reviewed retirement entry is invalid")

        branch = str(item.get("branch") or "").strip()
        retired_tip_sha = str(item.get("retired_tip_sha") or "").strip()
        replacement_sha = str(item.get("replacement_sha") or "").strip()
        rationale = str(item.get("rationale") or "").strip()

        if not branch or any(char in branch for char in "*?["):
            raise ValueError("reviewed retirement branch must be explicit")
        if branch in result:
            raise ValueError(f"duplicate reviewed retirement branch: {branch}")
        if not _SHA_RE.fullmatch(retired_tip_sha):
            raise ValueError(f"invalid retired tip SHA for {branch}")
        if not _SHA_RE.fullmatch(replacement_sha):
            raise ValueError(f"invalid replacement SHA for {branch}")
        if not rationale:
            raise ValueError(f"missing retirement rationale for {branch}")

        result[branch] = {
            "retired_tip_sha": retired_tip_sha,
            "replacement_sha": replacement_sha,
            "rationale": rationale,
        }
    return result


class GitHubApi:
    def __init__(self, repository: str, token: str):
        self.repository = repository
        self.token = token

    def request(self, method: str, path: str):
        request = Request(
            f"{API_ROOT}{path}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": API_VERSION,
                "User-Agent": "bible-bot-branch-hygiene",
            },
            method=method,
        )
        with urlopen(request, timeout=20) as response:
            raw = response.read()
        return json.loads(raw) if raw else None

    def _paged_list(self, path: str) -> list[dict]:
        values: list[dict] = []
        page = 1
        separator = "&" if "?" in path else "?"
        while True:
            batch = self.request(
                "GET",
                f"{path}{separator}per_page={PAGE_SIZE}&page={page}",
            )
            if not isinstance(batch, list):
                raise RuntimeError("GitHub paged response is not a list")
            values.extend(batch)
            if len(batch) < PAGE_SIZE:
                return values
            page += 1

    def pulls(self, state: str) -> list[dict]:
        return self._paged_list(f"/repos/{self.repository}/pulls?state={state}")

    def branches(self) -> list[dict]:
        return self._paged_list(f"/repos/{self.repository}/branches")

    def repository_metadata(self) -> dict:
        value = self.request("GET", f"/repos/{self.repository}")
        if not isinstance(value, dict):
            raise RuntimeError("GitHub repository response is malformed")
        return value

    def ref_sha(self, ref: str) -> str | None:
        encoded = quote(ref, safe="")
        try:
            value = self.request(
                "GET", f"/repos/{self.repository}/git/ref/heads/{encoded}"
            )
        except HTTPError as exc:
            if exc.code == 404:
                return None
            raise
        if not isinstance(value, dict):
            raise RuntimeError("GitHub ref response is malformed")
        obj = value.get("object") or {}
        sha = obj.get("sha")
        return str(sha) if sha else None

    def _compare(self, base_sha: str, head_sha: str) -> dict:
        value = self.request(
            "GET",
            f"/repos/{self.repository}/compare/{quote(base_sha, safe='')}...{quote(head_sha, safe='')}",
        )
        if not isinstance(value, dict):
            raise RuntimeError("GitHub compare response is malformed")
        return value

    def is_ancestor(self, base_sha: str, head_sha: str) -> bool:
        value = self._compare(base_sha, head_sha)
        status = str(value.get("status") or "")
        behind_by = value.get("behind_by")
        return status in {"ahead", "identical"} and behind_by == 0

    def content_sha(self, path: str, ref_sha: str) -> str | None:
        encoded_path = quote(path, safe="/")
        encoded_ref = quote(ref_sha, safe="")
        try:
            value = self.request(
                "GET",
                f"/repos/{self.repository}/contents/{encoded_path}?ref={encoded_ref}",
            )
        except HTTPError as exc:
            if exc.code == 404:
                return None
            raise
        if not isinstance(value, dict):
            raise RuntimeError("GitHub content response is malformed")
        sha = value.get("sha")
        return str(sha) if sha else None

    def patch_exactly_present_in(self, branch_sha: str, default_sha: str) -> bool:
        """Prove the branch-side patch tree is already represented in main."""
        value = self._compare(default_sha, branch_sha)
        files = value.get("files")
        if not isinstance(files, list) or len(files) >= _MAX_COMPARE_FILES:
            return False

        for item in files:
            if not isinstance(item, dict):
                return False
            filename = str(item.get("filename") or "")
            status = str(item.get("status") or "")
            branch_blob = str(item.get("sha") or "")
            if not filename:
                return False

            if status == "removed":
                if self.content_sha(filename, default_sha) is not None:
                    return False
                continue
            if status == "renamed":
                previous = str(item.get("previous_filename") or "")
                if not previous or not branch_blob:
                    return False
                if self.content_sha(filename, default_sha) != branch_blob:
                    return False
                if self.content_sha(previous, default_sha) is not None:
                    return False
                continue
            if status in {"added", "modified", "changed", "copied"}:
                if not branch_blob or self.content_sha(filename, default_sha) != branch_blob:
                    return False
                continue
            return False
        return True

    def delete_ref(self, ref: str) -> None:
        encoded = quote(ref, safe="")
        self.request("DELETE", f"/repos/{self.repository}/git/refs/heads/{encoded}")


def cleanup(
    api: GitHubApi,
    *,
    reviewed_retirements: dict[str, dict[str, str]] | None = None,
) -> tuple[list[str], list[str]]:
    metadata = api.repository_metadata()
    default_branch = str(metadata.get("default_branch") or "main")
    default_sha = api.ref_sha(default_branch)
    if not default_sha:
        raise RuntimeError("default branch ref is unavailable")

    closed = api.pulls("closed")
    opened = api.pulls("open")
    branches = api.branches()
    if reviewed_retirements is None:
        reviewed_retirements = _load_reviewed_retirements()

    merged_default_merge_shas = _merged_default_merge_shas(
        closed,
        repository=api.repository,
        default_branch=default_branch,
    )
    open_heads = _open_head_refs(opened, repository=api.repository)
    open_bases = _open_base_refs(opened, repository=api.repository)
    candidate_refs = sorted(
        {
            str(branch.get("name") or "")
            for branch in branches
            if _eligible_ref(
                str(branch.get("name") or ""),
                default_branch=default_branch,
            )
        }
    )

    deleted: list[str] = []
    skipped: list[str] = []
    for ref in candidate_refs:
        if ref in open_heads:
            skipped.append(f"{ref}: open PR exists")
            continue
        if ref in open_bases:
            skipped.append(f"{ref}: open PR base exists")
            continue

        current_sha = api.ref_sha(ref)
        if current_sha is None:
            continue

        merge_shas = merged_default_merge_shas.get((ref, current_sha), set())
        merged_default_match = any(
            merge_sha == default_sha or api.is_ancestor(merge_sha, default_sha)
            for merge_sha in merge_shas
        )
        ancestry_match = False
        exact_content_match = False
        reviewed_match = False

        if not merged_default_match:
            ancestry_match = api.is_ancestor(current_sha, default_sha)
            if not ancestry_match:
                exact_content_match = api.patch_exactly_present_in(
                    current_sha, default_sha
                )
                if not exact_content_match:
                    retirement = reviewed_retirements.get(ref)
                    if retirement is None:
                        skipped.append(
                            f"{ref}: unique content not proven in {default_branch}"
                        )
                        continue
                    if current_sha != retirement["retired_tip_sha"]:
                        skipped.append(f"{ref}: reviewed retirement SHA mismatch")
                        continue
                    replacement_sha = retirement["replacement_sha"]
                    if not api.is_ancestor(replacement_sha, default_sha):
                        skipped.append(
                            f"{ref}: reviewed replacement not in {default_branch}"
                        )
                        continue
                    reviewed_match = True

        # Re-read immediately before deletion. Never force-update a moved ref.
        confirmed_sha = api.ref_sha(ref)
        if confirmed_sha != current_sha:
            skipped.append(f"{ref}: ref moved during cleanup")
            continue

        if merged_default_match or ancestry_match or exact_content_match or reviewed_match:
            confirmed_default_sha = api.ref_sha(default_branch)
            if confirmed_default_sha != default_sha:
                skipped.append(
                    f"{ref}: {default_branch} moved during content proof"
                )
                continue

        api.delete_ref(ref)
        deleted.append(ref)

    return deleted, skipped


def main() -> int:
    repository = os.getenv("GITHUB_REPOSITORY", "").strip()
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not repository or not token:
        print("branch hygiene unavailable: GITHUB_REPOSITORY/GITHUB_TOKEN missing")
        return 2

    try:
        deleted, skipped = cleanup(GitHubApi(repository, token))
    except (HTTPError, OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(f"branch hygiene failed: {type(exc).__name__}")
        return 1

    print(f"branch hygiene: deleted={len(deleted)} skipped={len(skipped)}")
    for ref in deleted:
        print(f"deleted: {ref}")
    for reason in skipped:
        print(f"skipped: {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
