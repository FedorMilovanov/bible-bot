import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXECUTABLE_MANIFEST = ROOT / "data" / "reviewed-branch-retirements.json"
ARCHIVE = ROOT / "data" / "reviewed-branch-retirement-sweep-2026-08-17.json"


def test_reviewed_retirement_sweep_is_archive_only():
    assert not EXECUTABLE_MANIFEST.exists()

    payload = json.loads(ARCHIVE.read_text(encoding="utf-8"))
    assert payload["status"] == "ARCHIVED_AFTER_SUCCESSFUL_SWEEP"
    assert payload["cleanup_authority"] is False
    assert payload["sweep"] == {
        "workflow": "Branch Hygiene",
        "run_id": 32066983954,
        "successful_attempt": 1,
        "reviewed_historical_refs_deleted": 38,
    }
    assert payload["policy"]["records_are_audit_evidence_only"] is True
    assert payload["policy"]["records_must_not_authorize_future_ref_deletion"] is True
    assert payload["post_sweep_branch_inventory"] == ["main"]
