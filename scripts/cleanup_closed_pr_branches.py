#!/usr/bin/env python3
"""Delete stale repository work branches without losing unique history.

Automatic cleanup uses progressively stronger, independently checkable proofs:

1. current ref SHA exactly matches a closed same-repository PR head;
2. GitHub proves the current ref SHA is already an ancestor of captured main;
3. every branch-side changed path is byte-for-byte represented in captured main;
4. an exact-SHA branch has an explicitly reviewed
   ``SUPERSEDED_BY_STRONGER_MAIN`` disposition in the tracked audit manifest,
   and the manifest's reviewed main is still an ancestor of captured main.

A tracked ``MERGE_REQUIRED`` disposition always blocks deletion. Non-service
branch names are considered only when explicitly present in that manifest.
Every mutable ref used by a main-dependent proof is re-read immediately before
DELETE. The script never force-updates a ref.
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
ELIGIBLE_PREFIXES = ("agent/", "release/", "dependabot/")
PAGE_SIZE = 100
_MAX_COMPARE_FILES = 300
_DISPOSITION_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "historical-branch-dispositions.json"
)
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_ALLOWED_DISPOSITIONS = frozenset(
    {
        "SUPERSEDED_BY_STRONGER_MAIN",
        "ALREADY_MERGED_ANCESTOR",
        "MERGE_REQUIRED",
    }
)


def _eligible_ref(ref: str, *, default_branch: str) -> bool:
    return bool(ref) and ref != default_branch and ref.startswith(ELIGIBLE_PREFIXES)


def _closed_head_shas(
    closed_pulls: list[dict],
    *,
    repository: str,
    default_branch: str,
) -> dict[str, set[str]]:
    result: dict[str, set[str]] = defaultdict(set)
    for pull in closed_pulls:
        head = pull.get("head") or {}
        head_repo = head.get("repo") or {}
        ref = str(head.get("ref") or "")
        sha = str(head.get("sha") or "")
        if head_repo.get("full_name") != repository:
            continue
        if not _eligible_ref(ref, default_branch=default_branch) or not sha:
            continue
        result[ref].add(sha)
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


def _load_dispositions(path: Path = _DISPOSITION_PATH) -> tuple[str, dict[str, dict]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("historical branch disposition manifest cannot be loaded") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise RuntimeError("historical branch disposition schema is invalid")

    reviewed_main = payload.get("reviewed_against_main_sha")
    if not isinstance(reviewed_main, str) or _SHA_RE.fullmatch(reviewed_main) is None:
        raise RuntimeError("historical branch disposition reviewed main SHA is invalid")

    records = payload.get("records")
    if not isinstance(records, list):
        raise RuntimeError("historical branch disposition records are invalid")

    result: dict[str, dict] = {}
    for record in records:
        if not isinstance(record, dict):
            raise RuntimeError("historical branch disposition record is invalid")
        branch = record.get("branch")
        branch_sha = record.get("branch_sha")
        disposition = record.get("disposition")
        if not isinstance(branch, str) or not branch or branch.startswith("refs/"):
            raise RuntimeError("historical branch disposition branch is invalid")
        if branch in result:
            raise RuntimeError("historical branch disposition branch is duplicated")
        if not isinstance(branch_sha, str) or _SHA_RE.fullmatch(branch_sha) is None:
            raise RuntimeError("historical branch disposition SHA is invalid")
        if disposition not in _ALLOWED_DISPOSITIONS:
            raise RuntimeError("historical branch disposition value is invalid")
        summary = record.get("review_summary")
        evidence = record.get("replacement_evidence")
        if not isinstance(summary, str) or not summary.strip():
            raise RuntimeError("historical branch disposition review summary is missing")
        if not isinstance(evidence, list) or any(not isinstance(item, str) for item in evidence):
            raise RuntimeError("historical branch disposition evidence is invalid")
        result[branch] = record
    return reviewed_main, result


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


def cleanup(api: GitHubApi) -> tuple[list[str], list[str]]:
    metadata = api.repository_metadata()
    default_branch = str(metadata.get("default_branch") or "main")
    default_sha = api.ref_sha(default_branch)
    if not default_sha:
        raise RuntimeError("default branch ref is unavailable")

    reviewed_main, dispositions = _load_dispositions()
    if default_branch in dispositions:
        raise RuntimeError("default branch must not be a cleanup disposition")

    closed = api.pulls("closed")
    opened = api.pulls("open")
    branches = api.branches()

    closed_shas = _closed_head_shas(
        closed,
        repository=api.repository,
        default_branch=default_branch,
    )
    open_refs = _open_head_refs(opened, repository=api.repository)
    current_branch_names = {
        str(branch.get("name") or "") for branch in branches if branch.get("name")
    }
    candidate_refs = sorted(
        ref
        for ref in current_branch_names
        if _eligible_ref(ref, default_branch=default_branch) or ref in dispositions
    )

    deleted: list[str] = []
    skipped: list[str] = []
    for ref in candidate_refs:
        if ref in open_refs:
            skipped.append(f"{ref}: open PR exists")
            continue

        current_sha = api.ref_sha(ref)
        if current_sha is None:
            continue
        disposition = dispositions.get(ref)
        exact_disposition = (
            disposition
            if disposition is not None and disposition.get("branch_sha") == current_sha
            else None
        )
        if exact_disposition and exact_disposition.get("disposition") == "MERGE_REQUIRED":
            skipped.append(f"{ref}: manifest requires integration")
            continue

        closed_match = current_sha in closed_shas.get(ref, set())
        ancestry_match = False
        exact_content_match = False
        semantic_match = False
        if not closed_match:
            ancestry_match = api.is_ancestor(current_sha, default_sha)
            if not ancestry_match:
                exact_content_match = api.patch_exactly_present_in(current_sha, default_sha)
                if not exact_content_match and exact_disposition is not None:
                    if exact_disposition.get("disposition") == "SUPERSEDED_BY_STRONGER_MAIN":
                        semantic_match = api.is_ancestor(reviewed_main, default_sha)
                    elif exact_disposition.get("disposition") == "ALREADY_MERGED_ANCESTOR":
                        skipped.append(f"{ref}: manifest ancestor proof no longer holds")
                        continue
                if not exact_content_match and not semantic_match:
                    suffix = " (manifest SHA mismatch)" if disposition and not exact_disposition else ""
                    skipped.append(
                        f"{ref}: unique content not proven in {default_branch}{suffix}"
                    )
                    continue

        confirmed_sha = api.ref_sha(ref)
        if confirmed_sha != current_sha:
            skipped.append(f"{ref}: ref moved during cleanup")
            continue

        main_dependent = ancestry_match or exact_content_match or semantic_match
        if main_dependent:
            confirmed_default_sha = api.ref_sha(default_branch)
            if confirmed_default_sha != default_sha:
                skipped.append(f"{ref}: {default_branch} moved during cleanup proof")
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
