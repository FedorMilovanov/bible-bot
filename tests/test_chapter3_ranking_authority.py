import json
from pathlib import Path

import questions
from questions.chapter3.ranking_audit import (
    CHAPTER3_RANKING_HOLD_REASONS,
    CHAPTER3_RANKING_READY_IDS,
)
from questions.chapter3.ranking_authority import (
    CHAPTER3_RANKING_AUTHORIZED_IDS,
    CHAPTER3_RANKING_AUTHORITY,
)
from questions.chapter3.reviewed import CHAPTER3_REVIEWED_QUESTIONS

MANIFEST_PATH = Path(__file__).resolve().parents[1] / "data" / "chapter3-ranking-authority.json"
MANIFEST = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _ids(items):
    return {str(item["id"]) for item in items}


def test_explicit_authority_matches_dynamic_audit_ready_set_exactly():
    expected = {f"ch3_text_{number}" for number in range(101, 113)}
    assert CHAPTER3_RANKING_AUTHORIZED_IDS == expected
    assert CHAPTER3_RANKING_AUTHORIZED_IDS == CHAPTER3_RANKING_READY_IDS
    assert not CHAPTER3_RANKING_HOLD_REASONS


def test_authorized_cards_have_exact_objective_metadata_and_sources():
    reviewed = {item["id"]: item for item in CHAPTER3_REVIEWED_QUESTIONS}
    assert set(CHAPTER3_RANKING_AUTHORITY) == set(CHAPTER3_RANKING_AUTHORIZED_IDS)

    for qid in CHAPTER3_RANKING_AUTHORIZED_IDS:
        item = reviewed[qid]
        authority = CHAPTER3_RANKING_AUTHORITY[qid]
        assert item["claim_type"] == authority["claim_type"] == "text"
        assert item["confidence"] == authority["confidence"] == "high"
        assert item["position"] == authority["position"] == "neutral"
        assert item["competitive"] is True
        assert set(item["sources"]) == set(authority["required_sources"]) == {"sblgnt", "net_1p3_1_7"}
        assert authority["source_depth"] == {
            "sblgnt": "inspected_primary",
            "net_1p3_1_7": "inspected_passage",
        }
        assert authority["gameplay_admitted"] is False


def test_manifest_pins_same_twelve_ids_and_zero_holds():
    assert MANIFEST["status"] == "RANKING_IDS_AUTHORIZED_NOT_GAMEPLAY_ADMITTED"
    assert MANIFEST["parent_audit_head"] == "b695f9170673caaaff517803d82331386750ccb2"
    assert MANIFEST["authorized_count"] == 12
    assert set(MANIFEST["authorized_ids"]) == set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    assert MANIFEST["audit_holds_expected"] == 0
    assert MANIFEST["must_equal_dynamic_audit_ready_set"] is True
    assert MANIFEST["source_minimum"] == ["sblgnt", "net_1p3_1_7"]
    assert MANIFEST["source_depth"] == {
        "sblgnt": "inspected_primary",
        "net_1p3_1_7": "inspected_passage",
    }


def test_authority_checkpoint_still_makes_no_gameplay_change():
    authorized = set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    assert authorized.isdisjoint(_ids(questions.COMPETITIVE_POOL))
    assert authorized.isdisjoint(_ids(questions.BATTLE_POOL))
    for key, pool in questions.CHALLENGE_POOLS.items():
        assert authorized.isdisjoint(_ids(pool)), key

    assert MANIFEST["gameplay_admission"] is False
    assert MANIFEST["competitive_pool_mutated"] is False
    assert MANIFEST["battle_pool_mutated"] is False
    assert MANIFEST["challenge_pool_mutated"] is False


def test_authorized_cards_remain_available_in_normal_learning():
    chapter3_ids = _ids(questions.get_pool_by_key("chapter3"))
    assert CHAPTER3_RANKING_AUTHORIZED_IDS <= chapter3_ids
    assert len(chapter3_ids) == 165
