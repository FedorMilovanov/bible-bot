from collections import Counter

from questions import (
    BATTLE_POOL,
    CHALLENGE_POOLS,
    COMPETITIVE_POOL,
    POOL_REGISTRY,
    SOURCE_CATALOG,
)
from questions.chapter5.ranking_audit import (
    CHAPTER5_RANKING_AUDIT as RANKING_AUDIT,
    CHAPTER5_RANKING_HOLD_IDS,
    CHAPTER5_RANKING_READY_IDS,
)
from questions.chapter5.reviewed import CHAPTER5_REVIEWED_QUESTIONS
from questions.pool_policy import NON_SCORING_LEARNING_POOLS


def _ids(items):
    return {str(item["id"]) for item in items}


def test_chapter5_reviewed_count_and_answer_positions():
    assert len(CHAPTER5_REVIEWED_QUESTIONS) == 72
    assert Counter(card["correct"] for card in CHAPTER5_REVIEWED_QUESTIONS) == {
        0: 18,
        1: 18,
        2: 18,
        3: 18,
    }


def test_chapter5_has_unique_ids_questions_and_option_sets():
    ids = [card["id"] for card in CHAPTER5_REVIEWED_QUESTIONS]
    questions = [card["question"] for card in CHAPTER5_REVIEWED_QUESTIONS]
    option_sets = [tuple(card["options"]) for card in CHAPTER5_REVIEWED_QUESTIONS]
    assert len(ids) == len(set(ids))
    assert len(questions) == len(set(questions))
    assert len(option_sets) == len(set(option_sets))


def test_chapter5_is_normal_learning_only():
    ch5 = _ids(CHAPTER5_REVIEWED_QUESTIONS)
    assert "chapter5" in NON_SCORING_LEARNING_POOLS
    assert not ch5.intersection(_ids(POOL_REGISTRY["random_all"]))
    assert not ch5.intersection(_ids(COMPETITIVE_POOL))
    assert not ch5.intersection(_ids(BATTLE_POOL))
    assert all(
        not ch5.intersection(_ids(pool))
        for pool in CHALLENGE_POOLS.values()
    )


def test_ranking_audit_fails_closed():
    assert CHAPTER5_RANKING_READY_IDS == frozenset()
    assert len(CHAPTER5_RANKING_HOLD_IDS) == 72
    assert RANKING_AUDIT["ready"] == 0
    assert RANKING_AUDIT["hold"] == 72
    assert RANKING_AUDIT["battle_admission"] == 0
    assert RANKING_AUDIT["challenge_admission"] == 0


def test_root_source_registry_keeps_shared_identity_precedence_without_depth_upgrade():
    for source_id in {
        "w3n_williams_horrell_icc_v2_2023",
        "w3n_stanojevic_ecm_2021",
        "w3n_intf_ecm_catholic_controls",
        "w3i_sinaiticus_1p4_5",
    }:
        meta = SOURCE_CATALOG[source_id]
        assert meta["source_identity_only"] is True
        if "product_evidence_status" in meta:
            assert meta["product_evidence_status"] == "identity_only_lane_scoped"
        for forbidden_depth in (
            "inspection_scope",
            "evidence_status",
            "claim_inspection_edge_ids",
            "strongest_depth",
            "claim_depth",
        ):
            assert forbidden_depth not in meta
