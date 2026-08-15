import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data" / "historical-branch-dispositions.json"
REVIEWED_MAIN = "ae0058e795d7d2de56bf965bd5a87e75a7cc7268"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
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
ALLOWED_ARCHIVAL_DISPOSITIONS = {
    "SUPERSEDED_BY_STRONGER_MAIN",
    "ALREADY_MERGED_ANCESTOR",
}


def _payload() -> dict:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def test_tracked_manifest_is_complete_archival_evidence():
    payload = _payload()

    assert payload["schema_version"] == 2
    assert payload["status"] == "ARCHIVED_AFTER_SUCCESSFUL_SWEEP"
    assert payload["cleanup_authority"] is False
    assert payload["reviewed_against_main_sha"] == REVIEWED_MAIN
    assert payload["completed_cleanup"] == {
        "workflow": "Branch Hygiene",
        "run_id": 31859527454,
        "deleted": 26,
        "skipped": 0,
    }
    assert payload["policy"] == {
        "records_are_audit_evidence_only": True,
        "records_must_not_authorize_future_ref_deletion": True,
        "future_cleanup_requires_current_github_graph_or_tree_proof": True,
    }

    records = payload["records"]
    assert isinstance(records, list)
    assert {record["branch"] for record in records} == EXPECTED_BRANCHES
    assert len(records) == len(EXPECTED_BRANCHES)
    assert sum(
        record["disposition"] == "SUPERSEDED_BY_STRONGER_MAIN"
        for record in records
    ) == 24
    assert sum(
        record["disposition"] == "ALREADY_MERGED_ANCESTOR"
        for record in records
    ) == 1


def test_archival_records_keep_exact_identity_and_replacement_evidence():
    records = _payload()["records"]
    seen: set[str] = set()

    for record in records:
        branch = record["branch"]
        assert isinstance(branch, str) and branch and not branch.startswith("refs/")
        assert branch not in seen
        seen.add(branch)

        branch_sha = record["branch_sha"]
        assert isinstance(branch_sha, str) and SHA_RE.fullmatch(branch_sha)
        assert record["disposition"] in ALLOWED_ARCHIVAL_DISPOSITIONS
        assert isinstance(record["review_summary"], str)
        assert record["review_summary"].strip()

        evidence = record["replacement_evidence"]
        assert isinstance(evidence, list)
        assert all(isinstance(item, str) and item for item in evidence)
        for path in evidence:
            assert (ROOT / path).exists(), f"missing replacement evidence: {path}"


def test_archive_contains_no_future_merge_or_delete_instruction():
    payload = _payload()

    assert payload["cleanup_authority"] is False
    assert all(record["disposition"] != "MERGE_REQUIRED" for record in payload["records"])
    assert all("delete" not in record["review_summary"].lower() for record in payload["records"])
