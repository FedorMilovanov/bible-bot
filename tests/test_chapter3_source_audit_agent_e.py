import json
from pathlib import Path

from questions.chapter3.sources_crosscutting import CLAIMS, SOURCES, source_breakdown

MATRIX_PATH = Path(__file__).resolve().parents[1] / "data" / "chapter3-evidence-matrix-agent-e.json"
with MATRIX_PATH.open(encoding="utf-8") as fh:
    MATRIX = json.load(fh)
INTEGRATION = MATRIX["integration_audit"]

FROZEN = {
    "base": "9eefbae4cf91d178e9f488e695df9264478197c0",
    "A": "9ce110bf60e63a3332047a629762eb6214cbc569",
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


def test_agent_e_claim_matrix_is_noncompetitive():
    assert MATRIX["schema_version"] == 2
    assert len(CLAIMS) == 24
    assert all(claim["competitive_candidate"] is False for claim in CLAIMS)
    assert all(claim["source_ids"] and claim["limitations"] for claim in CLAIMS)


def test_final_cross_lane_frozen_snapshot_is_pinned_and_not_stale():
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


def test_cross_lane_ids_metadata_and_source_depth_are_clean():
    assert INTEGRATION["id_collisions"] == []
    assert INTEGRATION["canonical_metadata"] == {"A": True, "B": True, "C": True, "D": True}
    assert INTEGRATION["source_depth"] == {"A": "PASS", "B": "PASS", "C": "PASS", "D": "PASS"}


def test_answer_position_distributions_and_a_editorial_blocker_are_pinned():
    assert INTEGRATION["answer_positions"] == {
        "A": {"0": 16, "1": 15, "2": 13, "3": 12},
        "B": {"0": 10, "1": 9, "2": 9, "3": 9},
        "C": {"0": 7, "1": 7, "2": 7, "3": 6},
        "D": {"0": 12, "1": 11, "2": 11, "3": 11},
    }
    assert INTEGRATION["verdicts"] == {
        "A": "BLOCK",
        "B": "PASS_WITH_HOLD",
        "C": "PASS_WITH_HOLD",
        "D": "PASS_WITH_HOLD",
    }
    assert "Mechanical short repeating correct-position pattern" in INTEGRATION["blockers"]["A"][0]
    assert not INTEGRATION["blockers"]["B"]
    assert not INTEGRATION["blockers"]["C"]
    assert not INTEGRATION["blockers"]["D"]


def test_pass_with_hold_does_not_erase_substantive_holds():
    for lane in ("B", "C", "D"):
        assert INTEGRATION["verdicts"][lane] == "PASS_WITH_HOLD"
        assert INTEGRATION["holds"][lane]
    assert any(claim["status"] in {"CONTESTED", "HOLD"} for claim in CLAIMS)


def test_exact_lane_workflow_runs_are_recorded_successfully():
    expected_runs = {
        "A": {"CI": 1168, "Security Audit": 1048, "CodeQL Stacked PR": 850},
        "B": {"CI": 1183, "Security Audit": 1063, "CodeQL Stacked PR": 865},
        "C": {"CI": 1182, "Security Audit": 1062, "CodeQL Stacked PR": 864},
        "D": {"CI": 1194, "Security Audit": 1074, "CodeQL Stacked PR": 876},
    }
    for lane, workflows in expected_runs.items():
        for name, run_number in workflows.items():
            record = INTEGRATION["workflow_runs"][lane][name]
            assert record["run"] == run_number
            assert record["conclusion"] == "success"
