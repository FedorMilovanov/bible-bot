import json
from pathlib import Path

from questions.chapter3.sources_crosscutting import CLAIMS, SOURCES, source_breakdown

MATRIX_PATH = Path(__file__).resolve().parents[1] / "data" / "chapter3-evidence-matrix-agent-e.json"
with MATRIX_PATH.open(encoding="utf-8") as fh:
    MATRIX = json.load(fh)

INTEGRATION = MATRIX["integration_audit"]

FROZEN = {
    "base": "9eefbae4cf91d178e9f488e695df9264478197c0",
    "A": "8b194513420ae6dc5adf853b051539ca1f499ed0",
    "B": "b7cc829e31a0aefc851b12245d7933afcb6561e8",
    "C": "d64656dbcfb8c8c894a11d6cc5e764a189de9336",
    "D": "d4151176053aec4a6bce7685922cb90dfc5f2a77",
}


def test_agent_e_source_control_plane():
    assert 20 <= len(SOURCES) <= 40
    assert source_breakdown() == {
        "primary": 9,
        "academic_control": 8,
        "peer_reviewed": 7,
        "conservative": 12,
    }
    assert all(source["inspection_level"] and source["limitations"] for source in SOURCES.values())


def test_agent_e_claim_control_plane_is_noncompetitive():
    assert MATRIX["schema_version"] == 3
    assert MATRIX["claim_control_plane"]["authority"] == "questions/chapter3/sources_crosscutting.py"
    assert MATRIX["claim_control_plane"]["claim_count"] == 24
    assert len(CLAIMS) == 24
    assert all(claim["competitive_candidate"] is False for claim in CLAIMS)
    assert all(claim["source_ids"] and claim["limitations"] for claim in CLAIMS)
    assert set(MATRIX["claim_control_plane"]["status_by_claim_id"]) == {
        claim["claim_id"] for claim in CLAIMS
    }


def test_rerun_snapshot_is_pinned_and_not_stale():
    assert INTEGRATION["frozen_snapshot"] == FROZEN
    assert INTEGRATION["start_snapshot_match"] is True
    assert INTEGRATION["stale_audit"] is False
    assert INTEGRATION["question_count"] == {
        "A": 56,
        "B": 37,
        "C": 27,
        "D": 45,
        "total": 165,
    }
    rerun = INTEGRATION["rerun_scope"]
    assert rerun["previous_audited_sha"] == "9ce110bf60e63a3332047a629762eb6214cbc569"
    assert rerun["new_audited_sha"] == FROZEN["A"]
    assert rerun["compare_ahead_by"] == 8
    assert rerun["B_C_D_carried_forward_only_because_heads_unchanged"] is True
    assert rerun["A_source_catalog_blob_before"] == rerun["A_source_catalog_blob_after"]


def test_cross_lane_ids_metadata_and_source_depth_are_clean():
    assert INTEGRATION["id_collisions"] == []
    assert INTEGRATION["canonical_metadata"] == {"A": True, "B": True, "C": True, "D": True}
    assert INTEGRATION["source_depth"] == {"A": "PASS", "B": "PASS", "C": "PASS", "D": "PASS"}


def test_a_blocker_is_closed_without_hardcoding_one_target_sequence():
    assert INTEGRATION["answer_positions"]["A"] == {"0": 14, "1": 14, "2": 14, "3": 14}
    assert INTEGRATION["A_local_answer_positions"] == {
        "text": {"0": 3, "1": 3, "2": 3, "3": 3},
        "greek": {"0": 3, "1": 3, "2": 2, "3": 2},
        "intertext": {"0": 1, "1": 1, "2": 2, "3": 1},
        "history": {"0": 2, "1": 1, "2": 2, "3": 2},
        "theology": {"0": 2, "1": 2, "2": 2, "3": 2},
        "disputed": {"0": 1, "1": 2, "2": 1, "3": 2},
        "application": {"0": 2, "1": 2, "2": 2, "3": 2},
    }
    assert all(INTEGRATION["A_anti_pattern_checks"].values())
    assert INTEGRATION["blockers"] == {"A": [], "B": [], "C": [], "D": []}


def test_all_four_lanes_are_pass_with_hold_not_publication_ready():
    assert INTEGRATION["verdicts"] == {
        "A": "PASS_WITH_HOLD",
        "B": "PASS_WITH_HOLD",
        "C": "PASS_WITH_HOLD",
        "D": "PASS_WITH_HOLD",
    }
    for lane in ("A", "B", "C", "D"):
        assert INTEGRATION["holds"][lane]
    assert any(claim["status"] in {"CONTESTED", "HOLD"} for claim in CLAIMS)


def test_exact_lane_workflow_runs_are_recorded_successfully():
    expected_runs = {
        "A": {"CI": 1206, "Security Audit": 1086, "CodeQL Stacked PR": 888},
        "B": {"CI": 1183, "Security Audit": 1063, "CodeQL Stacked PR": 865},
        "C": {"CI": 1182, "Security Audit": 1062, "CodeQL Stacked PR": 864},
        "D": {"CI": 1194, "Security Audit": 1074, "CodeQL Stacked PR": 876},
    }
    for lane, workflows in expected_runs.items():
        for name, run_number in workflows.items():
            record = INTEGRATION["workflow_runs"][lane][name]
            assert record["run"] == run_number
            assert record["conclusion"] == "success"
