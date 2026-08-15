import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data" / "historical-branch-dispositions.json"
CLEANUP = ROOT / "scripts" / "cleanup_closed_pr_branches.py"


def test_historical_branch_dispositions_are_archival_evidence_only():
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))

    assert payload["schema_version"] == 2
    assert payload["status"] == "ARCHIVED_AFTER_SUCCESSFUL_SWEEP"
    assert payload["cleanup_authority"] is False
    assert payload["policy"] == {
        "records_are_audit_evidence_only": True,
        "records_must_not_authorize_future_ref_deletion": True,
        "future_cleanup_requires_current_github_graph_or_tree_proof": True,
    }
    assert payload["completed_cleanup"] == {
        "workflow": "Branch Hygiene",
        "run_id": 31859527454,
        "deleted": 26,
        "skipped": 0,
    }
    assert payload["records"]


def test_branch_cleanup_does_not_load_historical_semantic_dispositions():
    source = CLEANUP.read_text(encoding="utf-8")

    assert "historical-branch-dispositions.json" not in source
    assert "SUPERSEDED_BY_STRONGER_MAIN" not in source
    assert "MERGE_REQUIRED" not in source
    assert "_load_dispositions" not in source
