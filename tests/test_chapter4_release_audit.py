import json
from pathlib import Path

import questions
from questions.chapter4.product_sources import SOURCE_CATALOG as CHAPTER4_PRODUCT_SOURCE_IDENTITIES
from questions.chapter4.ranking_audit import CHAPTER4_RANKING_AUDIT, CHAPTER4_RANKING_READY_IDS
from questions.chapter4.research_handoff import CHAPTER4_RESEARCH_HANDOFF, RESEARCH_AUTHORITY_SHA
from questions.chapter4.reviewed import CHAPTER4_REVIEW_QUARANTINE_IDS, CHAPTER4_REVIEWED_QUESTIONS


ROOT = Path(__file__).resolve().parents[1]
AUDIT = json.loads((ROOT / "data" / "chapter4-release-audit.json").read_text(encoding="utf-8"))


def _ids(items):
    return {str(item.get("id") or "") for item in items}


def test_release_audit_pins_authority_and_zero_boundaries():
    assert RESEARCH_AUTHORITY_SHA == "0142430af8ba80f28e0fd9cde669d32611a1d2af"
    assert CHAPTER4_RESEARCH_HANDOFF["effective_count"] == 72
    assert CHAPTER4_RESEARCH_HANDOFF["research_prototype_count"] == 32
    assert CHAPTER4_RESEARCH_HANDOFF["research_hold_count"] == 0
    assert CHAPTER4_RESEARCH_HANDOFF["research_competitive_candidate_count"] == 0
    assert CHAPTER4_RESEARCH_HANDOFF["authored_count"] == 52
    assert len(CHAPTER4_REVIEWED_QUESTIONS) == 52
    assert CHAPTER4_REVIEW_QUARANTINE_IDS == frozenset()
    assert CHAPTER4_RANKING_READY_IDS == frozenset()
    assert CHAPTER4_RANKING_AUDIT["status"] == "LEARNING_ONLY_FAIL_CLOSED"
    assert CHAPTER4_RANKING_AUDIT["ready_count"] == 0
    assert CHAPTER4_RANKING_AUDIT["battle_count"] == 0
    assert CHAPTER4_RANKING_AUDIT["challenge_count"] == 0


def test_chapter4_is_reviewed_only_learning_pool_and_absent_from_ranking():
    assert questions.POOL_REGISTRY["chapter4"] == list(CHAPTER4_REVIEWED_QUESTIONS)
    chapter4_ids = _ids(CHAPTER4_REVIEWED_QUESTIONS)
    challenge_ids = set().union(*(_ids(pool) for pool in questions.CHALLENGE_POOLS.values()))

    assert chapter4_ids
    assert chapter4_ids.isdisjoint(_ids(questions.POOL_REGISTRY["random_all"]))
    assert chapter4_ids.isdisjoint(_ids(questions.COMPETITIVE_POOL))
    assert chapter4_ids.isdisjoint(_ids(questions.BATTLE_POOL))
    assert chapter4_ids.isdisjoint(challenge_ids)
    assert chapter4_ids.isdisjoint(_ids(questions.CHALLENGE_FALLBACK_POOL))


def test_chapter4_source_catalog_is_identity_only():
    forbidden_depth_keys = {
        "evidence_status",
        "inspection_level",
        "inspection_scope",
        "actual_inspection_depth",
    }
    assert CHAPTER4_PRODUCT_SOURCE_IDENTITIES
    for metadata in CHAPTER4_PRODUCT_SOURCE_IDENTITIES.values():
        assert metadata["source_identity_only"] is True
        assert metadata["research_authority_sha"] == RESEARCH_AUTHORITY_SHA
        assert "Identity/provenance only" in metadata["claim_limit"]
        assert forbidden_depth_keys.isdisjoint(metadata)


def test_release_manifest_matches_effective_product_contract():
    assert AUDIT["status"] == "CHAPTER4_RELEASE_AUDIT_READY_FOR_EXACT_HEAD_GATES"
    assert AUDIT["lineage"]["base"] == "e4dea87d7348ee940bc628f7f8d53379e05a5a3a"
    assert AUDIT["lineage"]["research"] == RESEARCH_AUTHORITY_SHA
    assert AUDIT["counts"] == {
        "effective_research_claims": 72,
        "research_mcq_prototypes": 32,
        "effective_research_holds": 0,
        "research_competitive_candidates": 0,
        "authored_cards": 52,
        "reviewed_cards": 52,
        "review_quarantine": 0,
        "ranking_ready": 0,
        "battle_admitted": 0,
        "challenge_admitted": 0,
    }
    assert AUDIT["normal_learning"]["pool_key"] == "chapter4"
    assert AUDIT["normal_learning"]["reviewed_only"] is True
    assert AUDIT["normal_learning"]["non_scoring"] is True
    assert AUDIT["normal_learning"]["random_all"] is False
    assert AUDIT["normal_learning"]["points"] == 0
    assert AUDIT["normal_learning"]["daily_bonus"] == 0
    assert AUDIT["normal_learning"]["achievements"] is False
    assert AUDIT["ranking_boundary"]["ranking_admission"] == 0
    assert AUDIT["ranking_boundary"]["battle_admission"] == 0
    assert AUDIT["ranking_boundary"]["challenge_admission"] == 0
    assert AUDIT["merge_authorized"] is False
    assert AUDIT["main_mutated"] is False
    assert AUDIT["release_audit_changes_gameplay"] is False
    assert AUDIT["remaining_product_holds"] == 0
    assert AUDIT["remaining_review_quarantine"] == 0
    assert AUDIT["remaining_ranking_admission"] == 0
