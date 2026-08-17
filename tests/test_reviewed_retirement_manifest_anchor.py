import json
from pathlib import Path


ARCHIVE = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "reviewed-branch-retirement-sweep-2026-08-17.json"
)
PR106_MERGE_SHA = "09ae51774c74c7cc78f2aae4748020b7218750ad"


def test_reviewed_retirement_archive_is_pinned_to_pr106_merge():
    payload = json.loads(ARCHIVE.read_text(encoding="utf-8"))

    assert payload["replacement_basis"] == {
        "pr": 106,
        "sha": PR106_MERGE_SHA,
    }
    assert payload["reviewed_retirement_source_pr"] == 118
    assert payload["sweep"]["reviewed_historical_refs_deleted"] == 38
    assert payload["cleanup_authority"] is False
