import json
from pathlib import Path

import questions
from questions.chapter3 import (
    CHAPTER3_DOMAIN_POOLS,
    CHAPTER3_STAGING_QUESTIONS,
)
from questions.chapter3.reviewed import (
    CHAPTER3_RANKING_CANDIDATE_IDS,
    CHAPTER3_REVIEWED_LANE_BY_ID,
    CHAPTER3_REVIEWED_QUESTIONS,
    CHAPTER3_REVIEW_QUARANTINE_IDS,
    reviewed_source_ids,
)
from questions.source_registry import SOURCE_CATALOG

MANIFEST_PATH = Path(__file__).resolve().parents[1] / "data" / "chapter3-review-manifest.json"
MANIFEST = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _ids(items):
    return {str(item["id"]) for item in items}


def test_reviewed_bank_is_complete_unique_and_object_isolated():
    staging_ids = [item["id"] for item in CHAPTER3_STAGING_QUESTIONS]
    reviewed_ids = [item["id"] for item in CHAPTER3_REVIEWED_QUESTIONS]

    assert len(staging_ids) == len(set(staging_ids)) == 165
    assert len(reviewed_ids) == len(set(reviewed_ids)) == 165
    assert reviewed_ids == staging_ids
    assert not CHAPTER3_REVIEW_QUARANTINE_IDS
    assert MANIFEST["staging_count"] == 165
    assert MANIFEST["reviewed_count"] == 165
    assert MANIFEST["review_quarantine_ids"] == []

    staging_by_id = {item["id"]: item for item in CHAPTER3_STAGING_QUESTIONS}
    for item in CHAPTER3_REVIEWED_QUESTIONS:
        assert item is not staging_by_id[item["id"]]
        assert item["options"] is not staging_by_id[item["id"]]["options"]


def test_reviewed_schema_and_epistemic_boundaries_hold():
    for item in CHAPTER3_REVIEWED_QUESTIONS:
        assert item["id"].startswith("ch3_")
        assert item["question"].strip()
        assert len(item["options"]) == 4
        assert len(item["options"]) == len(set(item["options"]))
        assert isinstance(item["correct"], int) and 0 <= item["correct"] < 4
        assert item["explanation"].strip()
        assert item["claim_type"] in {"text", "greek", "history", "interpretation", "application"}
        assert item["confidence"] in {"high", "medium", "contested"}
        assert item["position"] in {"neutral", "project"}
        assert isinstance(item["competitive"], bool)

        if item["position"] == "project":
            assert item["question"].startswith("[Позиция курса]")
            assert item["competitive"] is False
        if item["confidence"] == "contested":
            assert item["competitive"] is False
        if item["claim_type"] in {"greek", "history", "application"}:
            assert item["competitive"] is False


def test_reviewed_sources_resolve_without_cross_lane_depth_promotion():
    root_ids = set(SOURCE_CATALOG)
    assert len(CHAPTER3_REVIEWED_LANE_BY_ID) == 165

    for item in CHAPTER3_REVIEWED_QUESTIONS:
        assert item["sources"], item["id"]
        lane_local = reviewed_source_ids(item)
        unresolved = set(item["sources"]) - (root_ids | lane_local)
        assert not unresolved, (item["id"], unresolved)


def test_domain_coverage_matrix_is_explicit_and_nonempty():
    required = MANIFEST["coverage_required"]
    assert set(required) == set(CHAPTER3_DOMAIN_POOLS)

    for lane, required_domains in required.items():
        domains = CHAPTER3_DOMAIN_POOLS[lane]
        assert list(domains) == required_domains
        for domain in required_domains:
            assert domains[domain], (lane, domain)


def test_objective_ranking_candidates_are_internal_only():
    reviewed_by_id = {item["id"]: item for item in CHAPTER3_REVIEWED_QUESTIONS}
    assert CHAPTER3_RANKING_CANDIDATE_IDS

    for qid in CHAPTER3_RANKING_CANDIDATE_IDS:
        item = reviewed_by_id[qid]
        assert item["competitive"] is True
        assert item["position"] == "neutral"
        assert item["confidence"] != "contested"
        assert item["claim_type"] not in {"greek", "history", "application"}

    root_ranked_ids = _ids(questions.COMPETITIVE_POOL)
    assert CHAPTER3_RANKING_CANDIDATE_IDS.isdisjoint(root_ranked_ids)
    assert CHAPTER3_RANKING_CANDIDATE_IDS.isdisjoint(_ids(questions.BATTLE_POOL))
    for pool in questions.CHALLENGE_POOLS.values():
        assert CHAPTER3_RANKING_CANDIDATE_IDS.isdisjoint(_ids(pool))


def test_reviewed_bank_is_still_outside_root_product_registry():
    reviewed_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    assert "chapter3" not in questions.POOL_REGISTRY
    for key, pool in questions.POOL_REGISTRY.items():
        assert reviewed_ids.isdisjoint(_ids(pool)), key
    assert MANIFEST["product_wiring"] is False
    assert MANIFEST["normal_learning_authorized"] is False
    assert MANIFEST["ranking_authorized"] is False
