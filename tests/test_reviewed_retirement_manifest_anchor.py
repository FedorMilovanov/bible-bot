import json
from pathlib import Path


MANIFEST = Path(__file__).resolve().parents[1] / "data" / "reviewed-branch-retirements.json"
PR106_MERGE_SHA = "09ae51774c74c7cc78f2aae4748020b7218750ad"


def test_reviewed_retirement_manifest_is_pinned_to_pr106_merge():
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))

    assert payload["replacement_basis"] == {
        "sha": PR106_MERGE_SHA,
        "pr": 106,
        "summary": "Merged canonical runtime retirement; current main is a descendant.",
    }
    assert len(payload["retirements"]) == 38
    assert {item["replacement_sha"] for item in payload["retirements"]} == {
        PR106_MERGE_SHA
    }
