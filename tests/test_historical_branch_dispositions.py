import json
from pathlib import Path

import pytest

import scripts.cleanup_closed_pr_branches as hygiene


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data" / "historical-branch-dispositions.json"
REVIEWED_MAIN = "ae0058e795d7d2de56bf965bd5a87e75a7cc7268"
EXPECTED_BRANCHES = {
    "agent/battle-safe-cleanup",
    "agent/bible-bot-action-exception-audit",
    "agent/bible-bot-active-session-preflight-cli",
    "agent/bible-bot-answer-ledger-precondition",
    "agent/bible-bot-battle-delivery-audit",
    "agent/bible-bot-battle-migration-guard",
    "agent/bible-bot-block-generic-progress-overwrite",
    "agent/bible-bot-delivery-drain-audit",
    "agent/bible-bot-inaccuracy-report-foundation",
    "agent/bible-bot-live-migration-guard",
    "agent/bible-bot-live-migration-guard-v2",
    "agent/bible-bot-live-question-atomicity",
    "agent/bible-bot-option-binding",
    "agent/bible-bot-owner-cancel-cas",
    "agent/bible-bot-report-migration-guard",
    "agent/bible-bot-report-retry-metadata",
    "agent/bible-bot-report-submit-audit",
    "agent/bible-bot-restart-winner-regression",
    "agent/bible-bot-retention-preflight-readonly-v2",
    "agent/bible-bot-session-lifecycle-audit",
    "agent/bible-bot-session-migration-guard",
    "agent/legacy-report-outbox-backfill",
    "agent/wave10-failclosed-launch-candidate",
    "arena/019fe791-bible-bot",
    "qualitymarathon-temp",
}


def test_tracked_manifest_is_complete_and_evidence_paths_exist():
    reviewed_main, records = hygiene._load_dispositions(MANIFEST)

    assert reviewed_main == REVIEWED_MAIN
    assert set(records) == EXPECTED_BRANCHES
    assert sum(
        record["disposition"] == "SUPERSEDED_BY_STRONGER_MAIN"
        for record in records.values()
    ) == 24
    assert sum(
        record["disposition"] == "ALREADY_MERGED_ANCESTOR"
        for record in records.values()
    ) == 1
    assert all(record["disposition"] != "MERGE_REQUIRED" for record in records.values())

    for record in records.values():
        for evidence in record["replacement_evidence"]:
            assert (ROOT / evidence).exists(), f"missing replacement evidence: {evidence}"


def _manifest(tmp_path, *, records, reviewed_main="a" * 40):
    path = tmp_path / "dispositions.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "reviewed_against_main_sha": reviewed_main,
                "records": records,
            }
        ),
        encoding="utf-8",
    )
    return path


def _record(branch="agent/example", sha="b" * 40, disposition="SUPERSEDED_BY_STRONGER_MAIN"):
    return {
        "branch": branch,
        "branch_sha": sha,
        "disposition": disposition,
        "review_summary": "reviewed invariant is present in stronger main authority",
        "replacement_evidence": ["database.py"],
    }


def test_manifest_rejects_duplicate_branch(tmp_path):
    path = _manifest(tmp_path, records=[_record(), _record()])
    with pytest.raises(RuntimeError, match="duplicated"):
        hygiene._load_dispositions(path)


def test_manifest_rejects_invalid_exact_sha(tmp_path):
    path = _manifest(tmp_path, records=[_record(sha="short")])
    with pytest.raises(RuntimeError, match="SHA"):
        hygiene._load_dispositions(path)


def test_manifest_rejects_unknown_disposition(tmp_path):
    path = _manifest(tmp_path, records=[_record(disposition="DELETE_IT")])
    with pytest.raises(RuntimeError, match="value"):
        hygiene._load_dispositions(path)


class FakeApi:
    repository = "FedorMilovanov/bible-bot"

    def __init__(self, branch, sha, *, open_branch=False, reviewed_main_is_ancestor=True, move_branch=False, move_main=False):
        self.branch = branch
        self.sha = sha
        self.open_branch = open_branch
        self.reviewed_main_is_ancestor = reviewed_main_is_ancestor
        self.move_branch = move_branch
        self.move_main = move_main
        self.deleted = []
        self.branch_reads = 0
        self.main_reads = 0

    def repository_metadata(self):
        return {"default_branch": "main"}

    def ref_sha(self, ref):
        if ref == "main":
            self.main_reads += 1
            if self.move_main and self.main_reads > 1:
                return "f" * 40
            return "c" * 40
        if ref == self.branch:
            self.branch_reads += 1
            if self.move_branch and self.branch_reads > 1:
                return "e" * 40
            return self.sha
        return None

    def pulls(self, state):
        if state == "open" and self.open_branch:
            return [{"head": {"ref": self.branch, "sha": self.sha, "repo": {"full_name": self.repository}}}]
        return []

    def branches(self):
        return [{"name": "main"}, {"name": self.branch}]

    def is_ancestor(self, base_sha, head_sha):
        assert head_sha == "c" * 40
        if base_sha == REVIEWED_MAIN:
            return self.reviewed_main_is_ancestor
        return False

    def patch_exactly_present_in(self, branch_sha, default_sha):
        assert branch_sha == self.sha
        assert default_sha == "c" * 40
        return False

    def delete_ref(self, ref):
        self.deleted.append(ref)


def _semantic_manifest(branch, sha, disposition="SUPERSEDED_BY_STRONGER_MAIN"):
    return REVIEWED_MAIN, {
        branch: _record(branch=branch, sha=sha, disposition=disposition)
    }


def test_semantic_disposition_deletes_exact_sha_even_for_explicit_non_service_branch(monkeypatch):
    branch = "arena/reviewed-old-prototype"
    sha = "b" * 40
    monkeypatch.setattr(hygiene, "_load_dispositions", lambda: _semantic_manifest(branch, sha))
    api = FakeApi(branch, sha)

    deleted, skipped = hygiene.cleanup(api)

    assert deleted == [branch]
    assert skipped == []
    assert api.deleted == [branch]


def test_manifest_sha_mismatch_never_authorizes_semantic_delete(monkeypatch):
    branch = "arena/reviewed-old-prototype"
    current_sha = "b" * 40
    monkeypatch.setattr(
        hygiene,
        "_load_dispositions",
        lambda: _semantic_manifest(branch, "d" * 40),
    )
    api = FakeApi(branch, current_sha)

    deleted, skipped = hygiene.cleanup(api)

    assert deleted == []
    assert skipped == [f"{branch}: unique content not proven in main (manifest SHA mismatch)"]


def test_merge_required_disposition_always_blocks_delete(monkeypatch):
    branch = "agent/needs-integration"
    sha = "b" * 40
    monkeypatch.setattr(
        hygiene,
        "_load_dispositions",
        lambda: _semantic_manifest(branch, sha, "MERGE_REQUIRED"),
    )
    api = FakeApi(branch, sha, reviewed_main_is_ancestor=True)

    deleted, skipped = hygiene.cleanup(api)

    assert deleted == []
    assert skipped == [f"{branch}: manifest requires integration"]


def test_semantic_delete_requires_reviewed_main_to_remain_ancestor(monkeypatch):
    branch = "agent/reviewed"
    sha = "b" * 40
    monkeypatch.setattr(hygiene, "_load_dispositions", lambda: _semantic_manifest(branch, sha))
    api = FakeApi(branch, sha, reviewed_main_is_ancestor=False)

    deleted, skipped = hygiene.cleanup(api)

    assert deleted == []
    assert skipped == [f"{branch}: unique content not proven in main"]


def test_open_pr_blocks_manifest_cleanup(monkeypatch):
    branch = "agent/reviewed"
    sha = "b" * 40
    monkeypatch.setattr(hygiene, "_load_dispositions", lambda: _semantic_manifest(branch, sha))
    api = FakeApi(branch, sha, open_branch=True)

    deleted, skipped = hygiene.cleanup(api)

    assert deleted == []
    assert skipped == [f"{branch}: open PR exists"]


def test_branch_move_blocks_manifest_cleanup(monkeypatch):
    branch = "agent/reviewed"
    sha = "b" * 40
    monkeypatch.setattr(hygiene, "_load_dispositions", lambda: _semantic_manifest(branch, sha))
    api = FakeApi(branch, sha, move_branch=True)

    deleted, skipped = hygiene.cleanup(api)

    assert deleted == []
    assert skipped == [f"{branch}: ref moved during cleanup"]


def test_main_move_blocks_manifest_cleanup(monkeypatch):
    branch = "agent/reviewed"
    sha = "b" * 40
    monkeypatch.setattr(hygiene, "_load_dispositions", lambda: _semantic_manifest(branch, sha))
    api = FakeApi(branch, sha, move_main=True)

    deleted, skipped = hygiene.cleanup(api)

    assert deleted == []
    assert skipped == [f"{branch}: main moved during content proof"]
