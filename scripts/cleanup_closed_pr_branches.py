#!/usr/bin/env python3
"""Delete stale same-repository work branches that are still pinned to closed PR heads.

Safety rules:
- only service-owned prefixes are eligible;
- branches used by any open PR are never touched;
- a branch is deleted only if its *current* ref SHA still equals a SHA recorded
  on a closed PR for that same ref;
- main/default branches and fork branches are never touched.

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

    def pulls(self, state: str) -> list[dict]:
        pulls: list[dict] = []
        page = 1
        while True:
            batch = self.request(
                "GET",
                f"/repos/{self.repository}/pulls?state={state}&per_page={PAGE_SIZE}&page={page}",
            )
            if not isinstance(batch, list):
                raise RuntimeError("GitHub pull-list response is not a list")
            pulls.extend(batch)
            if len(batch) < PAGE_SIZE:
                return pulls
            page += 1

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

    def delete_ref(self, ref: str) -> None:
        encoded = quote(ref, safe="")
        self.request("DELETE", f"/repos/{self.repository}/git/refs/heads/{encoded}")


def cleanup(api: GitHubApi) -> tuple[list[str], list[str]]:
    metadata = api.repository_metadata()
    default_branch = str(metadata.get("default_branch") or "main")
    closed = api.pulls("closed")
    opened = api.pulls("open")

    closed_shas = _closed_head_shas(
        closed,
        repository=api.repository,
        default_branch=default_branch,
    )
    open_refs = _open_head_refs(opened, repository=api.repository)

    deleted: list[str] = []
    skipped: list[str] = []
    for ref in sorted(closed_shas):
        if ref in open_refs:
            skipped.append(f"{ref}: open PR exists")
            continue

        current_sha = api.ref_sha(ref)
        if current_sha is None:
            continue
        if current_sha not in closed_shas[ref]:
            skipped.append(f"{ref}: ref moved after closed PR")
            continue

        # Re-read immediately before deletion. We never force-update a ref and
        # refuse to delete if the branch has moved since the first read.
        confirmed_sha = api.ref_sha(ref)
        if confirmed_sha != current_sha:
            skipped.append(f"{ref}: ref moved during cleanup")
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
