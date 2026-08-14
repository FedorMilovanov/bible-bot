from collections import Counter, defaultdict

from questions import (
    BATTLE_POOL,
    CHALLENGE_POOLS,
    COMPETITIVE_POOL,
    POOL_REGISTRY,
    chapter5_questions,
)
from questions.chapter5.bank import CHAPTER5_STAGING_QUESTIONS
from questions.chapter5.ranking_audit import (
    CHAPTER5_RANKING_HOLD_IDS,
    CHAPTER5_RANKING_READY_IDS,
    RANKING_AUDIT,
)
from questions.chapter5.reviewed import (
    CHAPTER5_REVIEWED_QUESTIONS,
    CHAPTER5_REVIEW_QUARANTINE_IDS,
)
from questions.pool_policy import NON_SCORING_LEARNING_POOLS
from questions.source_registry import SOURCE_CATALOG


def _ids(items):
    return {item["id"] for item in items}


def test_release_counts_and_review_boundary():
    assert len(CHAPTER5_STAGING_QUESTIONS) == 72
    assert len(CHAPTER5_REVIEWED_QUESTIONS) == 72
    assert CHAPTER5_REVIEW_QUARANTINE_IDS == frozenset()
    assert len(chapter5_questions) == 72
    assert POOL_REGISTRY["chapter5"] == chapter5_questions


def test_reviewed_cards_are_deep_copy_isolated():
    assert CHAPTER5_REVIEWED_QUESTIONS is not CHAPTER5_STAGING_QUESTIONS
    for raw, reviewed in zip(
        CHAPTER5_STAGING_QUESTIONS,
        CHAPTER5_REVIEWED_QUESTIONS,
        strict=True,
    ):
        assert raw is not reviewed
        assert raw["options"] is not reviewed["options"]


def test_schema_ids_sources_and_visible_project_markers():
    ids = []
    for card in CHAPTER5_REVIEWED_QUESTIONS:
        ids.append(card["id"])
        assert len(card["options"]) == 4
        assert card["correct"] in {0, 1, 2, 3}
        assert card["claim_type"] in {
            "text",
            "greek",
            "history",
            "interpretation",
            "application",
        }
        assert card["confidence"] in {"high", "medium", "contested"}
        assert card["position"] in {"neutral", "project"}
        assert card["competitive"] is False
        assert card["sources"]
        if card["position"] == "project":
            assert card["question"].startswith("[Позиция курса]")
    assert len(ids) == len(set(ids)) == 72


def test_correct_index_distribution_global_and_subgroups():
    assert Counter(card["correct"] for card in CHAPTER5_REVIEWED_QUESTIONS) == {
        0: 18,
        1: 18,
        2: 18,
        3: 18,
    }
    by_type = defaultdict(list)
    by_verse = defaultdict(list)
    for card in CHAPTER5_REVIEWED_QUESTIONS:
        by_type[card["claim_type"]].append(card["correct"])
        by_verse[card["verse"]].append(card["correct"])
    for values in list(by_type.values()) + list(by_verse.values()):
        if len(values) >= 4:
            assert len(set(values)) >= 2


def test_no_duplicate_or_near_duplicate_exact_surfaces():
    questions = [
        " ".join(card["question"].lower().split())
        for card in CHAPTER5_REVIEWED_QUESTIONS
    ]
    option_sets = [
        tuple(sorted(" ".join(option.lower().split()) for option in card["options"]))
        for card in CHAPTER5_REVIEWED_QUESTIONS
    ]
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
