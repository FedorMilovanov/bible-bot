#!/usr/bin/env python3
"""Disable retired GitHub Actions registry records without broad registry mutation.

Two authorities are intentionally narrow:

* a reviewed manifest containing the twenty historical self-writing/one-shot
  workflows retired from this repository; and
* workflow files touched by the *currently closing* same-repository PR, but only
  when that path is absent from the captured default-branch tree. For an
  unmerged PR, an open PR touching the same workflow path blocks cleanup.

Unknown registry records are never disabled. Every disable is preceded by an
exact registry re-read and a default-branch race check, and is followed by a
state verification requiring ``disabled_manually``.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

API_ROOT = "https://api.github.com"
API_VERSION = "2022-11-28"
ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "data" / "retired-workflow-registry.json"
PAGE_SIZE = 100
DEPENDENCY_REVIEW_ID = 330647367
DEPENDABOT_DYNAMIC_ID = 333753975
PROTECTED_IDS = frozenset({DEPENDENCY_REVIEW_ID, DEPENDABOT_DYNAMIC_ID})
EXPECTED_RETIRED_IDS = frozenset(
    {
        330696854,
        331084699,
        331078803,
        331082912,
        331091062,
        331086729,
        330998221,
        331093764,
        330991119,
        330974563,
        331022610,
        331013380,
        330963188,
        331005508,
        331017660,
        330802612,
        330800213,
        330806993,
        330805217,
        330809962,
    }
)


@dataclass(frozen=True)
class RetiredWorkflow:
    id: int
    path: str
    name: str


def _is_workflow_path(path: str) -> bool:
    return path.startswith(".github/workflows/") and path.endswith((".yml", ".yaml"))


def load_manifest(path: Path = MANIFEST_PATH) -> tuple[RetiredWorkflow, ...]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema_version") != 1:
        raise ValueError("retired workflow manifest schema_version must be 1")
    values = raw.get("retired_workflows")
    if not isinstance(values, list):
        raise ValueError("retired_workflows must be a list")

    entries: list[RetiredWorkflow] = []
    seen_paths: set[str] = set()
    seen_ids: set[int] = set()
    for value in values:
        if not isinstance(value, dict):
            raise ValueError("retired workflow entry must be an object")
        workflow_id = value.get("id")
        workflow_path = value.get("path")
        name = value.get("name")
        if not isinstance(workflow_id, int) or workflow_id <= 0:
            raise ValueError("retired workflow id must be a positive integer")
        if not isinstance(workflow_path, str) or not _is_workflow_path(workflow_path):
            raise ValueError(f"invalid retired workflow path: {workflow_path!r}")
        if not isinstance(name, str) or not name:
            raise ValueError(f"invalid retired workflow name for id {workflow_id}")
        if workflow_id in PROTECTED_IDS:
            raise ValueError(f"protected workflow id cannot be retired: {workflow_id}")
        if workflow_id in seen_ids or workflow_path in seen_paths:
            raise ValueError("retired workflow manifest contains duplicate id/path")
        seen_ids.add(workflow_id)
        seen_paths.add(workflow_path)
        entries.append(RetiredWorkflow(workflow_id, workflow_path, name))

    if seen_ids != EXPECTED_RETIRED_IDS:
        raise ValueError(
            "retired workflow manifest must contain exactly the reviewed 20 workflow ids"
        )
    return tuple(entries)


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
                "User-Agent": "bible-bot-workflow-registry-hygiene",
            },
            method=method,
        )
        with urlopen(request, timeout=20) as response:
            payload = response.read()
        return json.loads(payload) if payload else None

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
        sha = (value.get("object") or {}).get("sha")
        return str(sha) if sha else None

    def content_exists(self, path: str, ref: str) -> bool:
        encoded_path = quote(path, safe="/")
        encoded_ref = quote(ref, safe="")
        try:
            self.request(
                "GET",
                f"/repos/{self.repository}/contents/{encoded_path}?ref={encoded_ref}",
            )
        except HTTPError as exc:
            if exc.code == 404:
                return False
            raise
        return True

    def workflows(self) -> list[dict]:
        result: list[dict] = []
        page = 1
        while True:
            value = self.request(
                "GET",
                f"/repos/{self.repository}/actions/workflows?per_page={PAGE_SIZE}&page={page}",
            )
            if not isinstance(value, dict) or not isinstance(value.get("workflows"), list):
                raise RuntimeError("GitHub workflow registry response is malformed")
            batch = value["workflows"]
            result.extend(batch)
            if len(batch) < PAGE_SIZE:
                return result
            page += 1

    def workflow(self, workflow_id: int) -> dict:
        value = self.request(
            "GET", f"/repos/{self.repository}/actions/workflows/{workflow_id}"
        )
        if not isinstance(value, dict):
            raise RuntimeError(f"workflow {workflow_id} response is malformed")
        return value

    def disable_workflow(self, workflow_id: int) -> None:
        self.request(
            "PUT", f"/repos/{self.repository}/actions/workflows/{workflow_id}/disable"
        )

    def pulls(self, state: str) -> list[dict]:
        result: list[dict] = []
        page = 1
        while True:
            value = self.request(
                "GET",
                f"/repos/{self.repository}/pulls?state={state}&per_page={PAGE_SIZE}&page={page}",
            )
            if not isinstance(value, list):
                raise RuntimeError("GitHub pulls response is malformed")
            result.extend(value)
            if len(value) < PAGE_SIZE:
                return result
            page += 1

    def pull_files(self, number: int) -> list[dict]:
        result: list[dict] = []
        page = 1
        while True:
            value = self.request(
                "GET",
                f"/repos/{self.repository}/pulls/{number}/files?per_page={PAGE_SIZE}&page={page}",
            )
            if not isinstance(value, list):
                raise RuntimeError("GitHub pull files response is malformed")
            result.extend(value)
            if len(value) < PAGE_SIZE:
                return result
            page += 1


def _closed_pr_candidate_paths(api: GitHubApi, event: dict | None) -> tuple[set[str], int | None, bool]:
    if not event or event.get("action") != "closed":
        return set(), None, False
    pull = event.get("pull_request")
    if not isinstance(pull, dict):
        return set(), None, False
    head_repo = ((pull.get("head") or {}).get("repo") or {}).get("full_name")
    if head_repo != api.repository:
        return set(), None, False
    number = pull.get("number") or event.get("number")
    if not isinstance(number, int):
        raise RuntimeError("closed pull request event has no numeric PR number")
    paths = {
        str(item.get("filename") or "")
        for item in api.pull_files(number)
        if _is_workflow_path(str(item.get("filename") or ""))
    }
    return paths, number, bool(pull.get("merged_at") or pull.get("merged"))


def _open_pr_touches_path(api: GitHubApi, path: str, *, exclude_number: int | None) -> bool:
    for pull in api.pulls("open"):
        number = pull.get("number")
        if not isinstance(number, int) or number == exclude_number:
            continue
        head_repo = ((pull.get("head") or {}).get("repo") or {}).get("full_name")
        if head_repo != api.repository:
            continue
        if any(str(item.get("filename") or "") == path for item in api.pull_files(number)):
            return True
    return False


def _assert_registry_match(actual: dict, expected: RetiredWorkflow) -> None:
    if actual.get("id") != expected.id:
        raise RuntimeError(f"workflow id mismatch for {expected.id}")
    if actual.get("path") != expected.path:
        raise RuntimeError(
            f"workflow {expected.id} path mismatch: {actual.get('path')!r} != {expected.path!r}"
        )
    if actual.get("name") != expected.name:
        raise RuntimeError(
            f"workflow {expected.id} name mismatch: {actual.get('name')!r} != {expected.name!r}"
        )


def _disable_verified(
    api: GitHubApi,
    *,
    workflow_id: int,
    expected_path: str,
    expected_name: str | None,
    captured_default_sha: str,
    default_branch: str,
) -> str:
    if workflow_id in PROTECTED_IDS:
        raise RuntimeError(f"refusing to disable protected workflow {workflow_id}")
    if api.ref_sha(default_branch) != captured_default_sha:
        raise RuntimeError(f"{default_branch} moved during workflow registry cleanup")
    if api.content_exists(expected_path, captured_default_sha):
        raise RuntimeError(
            f"refusing to disable workflow whose path exists in {default_branch}: {expected_path}"
        )

    current = api.workflow(workflow_id)
    if current.get("id") != workflow_id or current.get("path") != expected_path:
        raise RuntimeError(f"workflow {workflow_id} changed identity before disable")
    if expected_name is not None and current.get("name") != expected_name:
        raise RuntimeError(f"workflow {workflow_id} changed name before disable")

    state = current.get("state")
    if state == "disabled_manually":
        return "already_disabled"
    if state != "active":
        raise RuntimeError(f"workflow {workflow_id} has unexpected state {state!r}")

    api.disable_workflow(workflow_id)
    after = api.workflow(workflow_id)
    if (
        after.get("id") != workflow_id
        or after.get("path") != expected_path
        or after.get("state") != "disabled_manually"
    ):
        raise RuntimeError(f"workflow {workflow_id} disable was not verified")
    if expected_name is not None and after.get("name") != expected_name:
        raise RuntimeError(f"workflow {workflow_id} changed name after disable")
    return "disabled"


def cleanup_registry(
    api: GitHubApi,
    *,
    manifest_path: Path = MANIFEST_PATH,
    event: dict | None = None,
) -> dict[str, int]:
    manifest = load_manifest(manifest_path)
    metadata = api.repository_metadata()
    default_branch = str(metadata.get("default_branch") or "main")
    captured_default_sha = api.ref_sha(default_branch)
    if not captured_default_sha:
        raise RuntimeError("default branch ref is unavailable")

    registry = api.workflows()
    by_id: dict[int, dict] = {}
    by_path: dict[str, list[dict]] = {}
    for item in registry:
        workflow_id = item.get("id")
        workflow_path = item.get("path")
        if not isinstance(workflow_id, int) or not isinstance(workflow_path, str):
            raise RuntimeError("workflow registry contains malformed identity")
        if workflow_id in by_id:
            raise RuntimeError(f"workflow registry contains duplicate id {workflow_id}")
        by_id[workflow_id] = item
        by_path.setdefault(workflow_path, []).append(item)

    counts = {"disabled": 0, "already_disabled": 0, "missing": 0, "event_disabled": 0}
    processed_ids: set[int] = set()

    for expected in manifest:
        current = by_id.get(expected.id)
        if current is None:
            counts["missing"] += 1
            continue
        _assert_registry_match(current, expected)
        result = _disable_verified(
            api,
            workflow_id=expected.id,
            expected_path=expected.path,
            expected_name=expected.name,
            captured_default_sha=captured_default_sha,
            default_branch=default_branch,
        )
        counts[result] += 1
        processed_ids.add(expected.id)

    event_paths, closed_number, merged = _closed_pr_candidate_paths(api, event)
    for workflow_path in sorted(event_paths):
        if api.content_exists(workflow_path, captured_default_sha):
            continue
        if not merged and _open_pr_touches_path(
            api, workflow_path, exclude_number=closed_number
        ):
            continue
        for current in by_path.get(workflow_path, []):
            workflow_id = current["id"]
            if workflow_id in processed_ids or workflow_id in PROTECTED_IDS:
                continue
            result = _disable_verified(
                api,
                workflow_id=workflow_id,
                expected_path=workflow_path,
                expected_name=None,
                captured_default_sha=captured_default_sha,
                default_branch=default_branch,
            )
            if result == "disabled":
                counts["event_disabled"] += 1
            else:
                counts["already_disabled"] += 1
            processed_ids.add(workflow_id)

    return counts


def _load_event() -> dict | None:
    event_path = os.getenv("GITHUB_EVENT_PATH", "").strip()
    if not event_path:
        return None
    value = json.loads(Path(event_path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("GitHub event payload is malformed")
    return value


def main() -> int:
    repository = os.getenv("GITHUB_REPOSITORY", "").strip()
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not repository or not token:
        print("workflow registry hygiene unavailable: GITHUB_REPOSITORY/GITHUB_TOKEN missing")
        return 2
    try:
        counts = cleanup_registry(
            GitHubApi(repository, token),
            event=_load_event(),
        )
    except (HTTPError, OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(f"workflow registry hygiene failed: {type(exc).__name__}: {exc}")
        return 1

    print(
        "workflow registry hygiene: "
        f"disabled={counts['disabled']} "
        f"already_disabled={counts['already_disabled']} "
        f"missing={counts['missing']} "
        f"event_disabled={counts['event_disabled']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
