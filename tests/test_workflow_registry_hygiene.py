import json
from pathlib import Path

import pytest

from scripts.cleanup_retired_workflow_registry import (
    DEPENDABOT_DYNAMIC_ID,
    DEPENDENCY_REVIEW_ID,
    EXPECTED_RETIRED_IDS,
    RetiredWorkflow,
    _closed_pr_candidate_paths,
    cleanup_registry,
    load_manifest,
)

REPO = "FedorMilovanov/bible-bot"


def _registry_item(entry: RetiredWorkflow, *, state="active"):
    return {
        "id": entry.id,
        "path": entry.path,
        "name": entry.name,
        "state": state,
    }


def _event(number=109, *, merged=False, head_repo=REPO):
    return {
        "action": "closed",
        "number": number,
        "pull_request": {
            "number": number,
            "merged": merged,
            "merged_at": "2026-08-17T00:00:00Z" if merged else None,
            "head": {"repo": {"full_name": head_repo}},
        },
    }


class FakeApi:
    repository = REPO

    def __init__(
        self,
        *,
        registry=None,
        present_paths=None,
        pull_files=None,
        open_pulls=None,
        ref_reads=None,
    ):
        manifest = load_manifest()
        self.registry = {
            item["id"]: dict(item)
            for item in (
                registry
                if registry is not None
                else [_registry_item(entry) for entry in manifest]
            )
        }
        self.present_paths = set(present_paths or [])
        self.pull_file_map = dict(pull_files or {})
        self.open_pulls = list(open_pulls or [])
        self.ref_reads = list(ref_reads or ["main-sha"] * 100)
        self.disable_calls = []

    def repository_metadata(self):
        return {"default_branch": "main"}

    def ref_sha(self, ref):
        assert ref == "main"
        return self.ref_reads.pop(0) if self.ref_reads else "main-sha"

    def content_exists(self, path, ref):
        assert ref == "main-sha"
        return path in self.present_paths

    def workflows(self):
        return [dict(item) for item in self.registry.values()]

    def workflow(self, workflow_id):
        return dict(self.registry[workflow_id])

    def disable_workflow(self, workflow_id):
        self.disable_calls.append(workflow_id)
        self.registry[workflow_id]["state"] = "disabled_manually"

    def pull_files(self, number):
        return list(self.pull_file_map.get(number, []))

    def pulls(self, state):
        assert state == "open"
        return list(self.open_pulls)


def test_reviewed_manifest_is_exactly_twenty_retired_workflows():
    entries = load_manifest()
    ids = {entry.id for entry in entries}
    assert len(entries) == 20
    assert ids == EXPECTED_RETIRED_IDS
    assert DEPENDENCY_REVIEW_ID not in ids
    assert DEPENDABOT_DYNAMIC_ID not in ids
    assert all(entry.path.startswith(".github/workflows/") for entry in entries)
    assert all(not entry.path.startswith("dynamic/") for entry in entries)


def test_manifest_fails_closed_if_reviewed_id_set_changes(tmp_path: Path):
    source = json.loads(
        Path("data/retired-workflow-registry.json").read_text(encoding="utf-8")
    )
    source["retired_workflows"] = source["retired_workflows"][:-1]
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(source), encoding="utf-8")
    with pytest.raises(ValueError, match="exactly the reviewed 20"):
        load_manifest(path)


def test_cleanup_disables_and_verifies_all_twenty_manifest_records():
    api = FakeApi()
    counts = cleanup_registry(api)
    assert counts == {
        "disabled": 20,
        "already_disabled": 0,
        "missing": 0,
        "event_disabled": 0,
    }
    assert set(api.disable_calls) == EXPECTED_RETIRED_IDS
    assert all(
        api.registry[workflow_id]["state"] == "disabled_manually"
        for workflow_id in EXPECTED_RETIRED_IDS
    )


def test_cleanup_is_idempotent_for_already_disabled_record():
    manifest = load_manifest()
    registry = [_registry_item(entry) for entry in manifest]
    registry[0]["state"] = "disabled_manually"
    already_id = registry[0]["id"]
    api = FakeApi(registry=registry)
    counts = cleanup_registry(api)
    assert counts["disabled"] == 19
    assert counts["already_disabled"] == 1
    assert already_id not in api.disable_calls


def test_cleanup_fails_closed_on_registry_name_mismatch():
    manifest = load_manifest()
    registry = [_registry_item(entry) for entry in manifest]
    registry[0]["name"] = "unexpected renamed workflow"
    api = FakeApi(registry=registry)
    with pytest.raises(RuntimeError, match="name mismatch"):
        cleanup_registry(api)
    assert api.disable_calls == []


def test_cleanup_refuses_to_disable_path_present_in_current_main():
    manifest = load_manifest()
    api = FakeApi(present_paths={manifest[0].path})
    with pytest.raises(RuntimeError, match="path exists in main"):
        cleanup_registry(api)
    assert api.disable_calls == []


def test_cleanup_fails_if_default_branch_moves_before_disable():
    api = FakeApi(ref_reads=["main-sha", "moved-main"])
    with pytest.raises(RuntimeError, match="main moved"):
        cleanup_registry(api)
    assert api.disable_calls == []


def test_unknown_registry_record_is_untouched_without_closed_pr_authority():
    manifest = load_manifest()
    unknown = {
        "id": 999000001,
        "path": ".github/workflows/temporary-review.yml",
        "name": "Temporary review",
        "state": "active",
    }
    api = FakeApi(registry=[*[_registry_item(e) for e in manifest], unknown])
    cleanup_registry(api)
    assert 999000001 not in api.disable_calls
    assert api.registry[999000001]["state"] == "active"


def test_closed_same_repo_pr_can_disable_only_its_absent_workflow_path():
    manifest = load_manifest()
    unknown = {
        "id": 999000001,
        "path": ".github/workflows/temporary-review.yml",
        "name": "Temporary review",
        "state": "active",
    }
    api = FakeApi(
        registry=[*[_registry_item(e) for e in manifest], unknown],
        pull_files={
            109: [
                {"filename": ".github/workflows/temporary-review.yml"},
                {"filename": "ordinary.py"},
            ]
        },
    )
    counts = cleanup_registry(api, event=_event(109, merged=True))
    assert counts["event_disabled"] == 1
    assert api.registry[999000001]["state"] == "disabled_manually"


def test_unmerged_closed_pr_does_not_disable_path_touched_by_other_open_pr():
    manifest = load_manifest()
    unknown = {
        "id": 999000001,
        "path": ".github/workflows/temporary-review.yml",
        "name": "Temporary review",
        "state": "active",
    }
    api = FakeApi(
        registry=[*[_registry_item(e) for e in manifest], unknown],
        pull_files={
            109: [{"filename": ".github/workflows/temporary-review.yml"}],
            110: [{"filename": ".github/workflows/temporary-review.yml"}],
        },
        open_pulls=[
            {"number": 110, "head": {"repo": {"full_name": REPO}}},
        ],
    )
    counts = cleanup_registry(api, event=_event(109, merged=False))
    assert counts["event_disabled"] == 0
    assert api.registry[999000001]["state"] == "active"


def test_closed_fork_pr_is_not_registry_cleanup_authority():
    api = FakeApi(pull_files={109: [{"filename": ".github/workflows/temp.yml"}]})
    paths, number, merged = _closed_pr_candidate_paths(
        api, _event(109, merged=False, head_repo="someone/fork")
    )
    assert paths == set()
    assert number is None
    assert merged is False
    assert api.pull_file_map[109]


def test_dependency_review_record_is_never_event_disabled():
    manifest = load_manifest()
    dependency_review = {
        "id": DEPENDENCY_REVIEW_ID,
        "path": ".github/workflows/dependency-review.yml",
        "name": "Dependency Review",
        "state": "active",
    }
    api = FakeApi(
        registry=[*[_registry_item(e) for e in manifest], dependency_review],
        pull_files={109: [{"filename": ".github/workflows/dependency-review.yml"}]},
    )
    cleanup_registry(api, event=_event(109, merged=True))
    assert DEPENDENCY_REVIEW_ID not in api.disable_calls
    assert api.registry[DEPENDENCY_REVIEW_ID]["state"] == "active"


def test_duplicate_registry_id_fails_closed():
    manifest = load_manifest()
    api = FakeApi()
    first = _registry_item(manifest[0])
    api.workflows = lambda: [first, dict(first)]
    with pytest.raises(RuntimeError, match="duplicate id"):
        cleanup_registry(api)
