#!/usr/bin/env python3
"""Delete stale same-repository service work branches without losing unique history.

Safety rules:
- only service-owned prefixes are eligible;
- branches used by any open PR are never touched;
- a branch is deletable when one of three independent proofs succeeds:
  1. its current ref SHA still equals a SHA recorded on a closed PR for that
     same ref;
  2. GitHub's commit graph proves its current SHA is already an ancestor of
     the captured default-branch SHA; or
  3. every path changed by the branch relative to its merge-base is already
     byte-for-byte represented in the captured default-branch tree (including
     removals and both sides of renames).
- branches with any unmatched content remain untouched;
- branch SHA is re-read immediately before DELETE;
- for graph/content proofs, the default-branch SHA is also re-read before
  DELETE and the deletion is refused if main moved during the check;
- main/default branches and fork heads are never touched.

The script uses only the GitHub Actions GITHUB_TOKEN. It never force-updates refs.
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

API_ROOT = "https://api.github.com"
API_VERSION = "2022-11-28"
ELIGIBLE_PREFIXES = ("agent/", "release/", "dependabot/")
PAGE_SIZE = 100
_MAX_COMPARE_FILES = 300


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
        """Prove the branch-side patch tree is already represented in main.

        GitHub's three-dot compare reports files changed by ``branch_sha`` from
        the merge-base with ``default_sha``. We compare each resulting Git blob
        (or deletion) against the captured default tree. This is intentionally
        stronger than semantic similarity: one unmatched byte keeps the branch.
        """
        value = self._compare(default_sha, branch_sha)
        files = value.get("files")
        if not isinstance(files, list):
            return False
        # GitHub caps compare file results at 300. Exactly hitting the cap is
        # ambiguous, so fail closed instead of proving an incomplete footprint.
        if len(files) >= _MAX_COMPARE_FILES:
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
                if not branch_blob:
                    return False
                if self.content_sha(filename, default_sha) != branch_blob:
                    return False
                continue

            # Unknown/new GitHub statuses are not cleanup authority.
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

    closed = api.pulls("closed")
    opened = api.pulls("open")
    branches = api.branches()

    closed_shas = _closed_head_shas(
        closed,
        repository=api.repository,
        default_branch=default_branch,
    )
    open_refs = _open_head_refs(opened, repository=api.repository)
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
        if ref in open_refs:
            skipped.append(f"{ref}: open PR exists")
            continue

        current_sha = api.ref_sha(ref)
        if current_sha is None:
            continue

        closed_match = current_sha in closed_shas.get(ref, set())
        ancestry_match = False
        exact_content_match = False
        if not closed_match:
            ancestry_match = api.is_ancestor(current_sha, default_sha)
            if not ancestry_match:
                exact_content_match = api.patch_exactly_present_in(
                    current_sha, default_sha
                )
                if not exact_content_match:
                    skipped.append(
                        f"{ref}: unique content not proven in {default_branch}"
                    )
                    continue

        # Re-read immediately before deletion. We never force-update a ref and
        # refuse to delete if the branch has moved since the proof was made.
        confirmed_sha = api.ref_sha(ref)
        if confirmed_sha != current_sha:
            skipped.append(f"{ref}: ref moved during cleanup")
            continue

        if ancestry_match or exact_content_match:
            confirmed_default_sha = api.ref_sha(default_branch)
            if confirmed_default_sha != default_sha:
                skipped.append(f"{ref}: {default_branch} moved during content proof")
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
