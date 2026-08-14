import json
from pathlib import Path

import questions
from questions.chapter4.product_dispositions import CHAPTER4_PRODUCT_DISPOSITIONS
from questions.chapter4.prototype_crosswalk import CHAPTER4_PROTOTYPE_CROSSWALK
from questions.chapter4.ranking_audit import CHAPTER4_RANKING_AUDIT, CHAPTER4_RANKING_READY_IDS
from questions.chapter4.ranking_review import W3Q123_RANKING_REVIEW
from questions.chapter4.research_handoff import (
    RESEARCH_AUTHORITY_DIGEST_SHA256,
    RESEARCH_AUTHORITY_SHA,
    RESEARCH_HANDOFF_SCHEMA_VERSION,
    RESEARCH_HANDOFF_V2,
    RESEARCH_REPOSITORY,
)
from questions.chapter4.review_registry import PRODUCT_REVIEW_BY_CARD_ID
from questions.chapter4.reviewed import (
    CHAPTER4_REVIEW_QUARANTINE_IDS,
    CHAPTER4_REVIEWED_QUESTIONS,
)


ROOT = Path(__file__).resolve().parents[1]
AUDIT = json.loads((ROOT / "data" / "chapter4-release-audit.json").read_text(encoding="utf-8"))


def _ids(items):
    return {str(item.get("id") or "") for item in items}


def test_release_audit_v2_pins_exact_immutable_research_authority():
    assert AUDIT["schema_version"] == 2
    assert RESEARCH_HANDOFF_SCHEMA_VERSION == 2
    assert RESEARCH_REPOSITORY == "FedorMilovanov/Research"
    assert RESEARCH_AUTHORITY_SHA == "7e0140129a4aba59a09737701967c3820ff1af57"
    assert RESEARCH_AUTHORITY_DIGEST_SHA256 == "1f444991ecc2f180abdbe0f459148ba8dbf0a5045b1d8888e462683c78366c7d"
    assert AUDIT["lineage"]["research_repository"] == RESEARCH_REPOSITORY
    assert AUDIT["lineage"]["research_authority_sha"] == RESEARCH_AUTHORITY_SHA
    assert AUDIT["lineage"]["research_authority_digest_sha256"] == RESEARCH_AUTHORITY_DIGEST_SHA256
    assert AUDIT["lineage"]["research_handoff_schema_version"] == 2


def test_release_counts_cover_cards_claims_prototypes_and_ranking():
    counts = AUDIT["counts"]
    assert len(RESEARCH_HANDOFF_V2) == counts["effective_research_claims"] == 72
    assert len(PRODUCT_REVIEW_BY_CARD_ID) == counts["product_review_records_v2"] == 52
    assert len(CHAPTER4_REVIEWED_QUESTIONS) == counts["reviewed_cards"] == 52
    assert CHAPTER4_REVIEW_QUARANTINE_IDS == frozenset()
    assert len(CHAPTER4_PRODUCT_DISPOSITIONS) == 72
    assert len(CHAPTER4_PROTOTYPE_CROSSWALK) == counts["research_mcq_prototypes"] == 32
    assert counts["product_card_dispositions"] == 52
    assert counts["retained_research_support_dispositions"] == 20
    assert counts["prototype_safe_template"] == 13
    assert counts["prototype_needs_rewrite"] == 10
    assert counts["prototype_noncompetitive_only"] == 6
    assert counts["prototype_rejected_reference_drift"] == 3
    assert CHAPTER4_RANKING_READY_IDS == frozenset()
    assert CHAPTER4_RANKING_AUDIT["ready_count"] == counts["ranking_ready"] == 0
    assert W3Q123_RANKING_REVIEW["product_ranking_decision"] == "NO_RANKING_ADMISSION"


def test_release_audit_preserves_epistemic_and_source_boundaries():
    boundaries = AUDIT["epistemic_boundaries"]
    assert all(boundaries.values())
    source = AUDIT["source_boundary"]
    assert source["root_registry"] == "IDENTITY_ONLY"
    assert source["global_strongest_depth_field"] is False
    assert source["existing_root_authority_overwritten"] is False
    assert source["claim_level_source_ids_and_edge_ids_live_in_private_review_registry"] is True


def test_chapter4_is_reviewed_only_learning_pool_and_absent_from_gameplay():
    assert questions.POOL_REGISTRY["chapter4"] == list(CHAPTER4_REVIEWED_QUESTIONS)
    chapter4_ids = _ids(CHAPTER4_REVIEWED_QUESTIONS)
    challenge_ids = set().union(*(_ids(pool) for pool in questions.CHALLENGE_POOLS.values()))
    assert chapter4_ids
    assert chapter4_ids.isdisjoint(_ids(questions.POOL_REGISTRY["random_all"]))
    assert chapter4_ids.isdisjoint(_ids(questions.COMPETITIVE_POOL))
    assert chapter4_ids.isdisjoint(_ids(questions.BATTLE_POOL))
    assert chapter4_ids.isdisjoint(challenge_ids)
    assert chapter4_ids.isdisjoint(_ids(questions.CHALLENGE_FALLBACK_POOL))
    learning = AUDIT["normal_learning"]
    assert learning["non_scoring"] is True
    assert learning["points"] == 0
    assert learning["daily_bonus"] == 0
    assert learning["achievements"] is False
    assert learning["ranked_total_counters"] is False
    assert learning["perfect_counters"] is False
    assert learning["progress_keys"] == [
        "chapter4_attempts",
        "chapter4_correct",
        "chapter4_total",
        "chapter4_best_score",
    ]


def test_release_audit_records_all_required_adversarial_negative_tests():
    required = {
        "forged_research_claim_id",
        "wrong_effective_claim_digest",
        "swapped_claim_inspection_edge_id",
        "source_without_edge",
        "project_to_neutral",
        "confidence_promotion",
        "interpretation_to_text",
        "competitive_flag",
        "random_all_injection",
        "competitive_pool_injection",
        "battle_injection",
        "challenge_injection",
        "challenge_fallback_injection",
        "public_correct_answer_leak",
        "public_review_metadata_leak",
    }
    assert required <= set(AUDIT["negative_tests"])


def test_second_adversarial_pass_is_explicitly_sequenced_after_first_green():
    second = AUDIT["second_adversarial_content_pass"]
    assert second["status"] in {"PENDING_AFTER_FIRST_GREEN", "COMPLETE"}
    assert second["required_cards"] == 52
    if second["status"] == "COMPLETE":
        assert second["reviewed_cards"] == 52
        assert second["open_findings"] == 0


def test_merge_boundary_remains_agent5_only():
    assert AUDIT["merge_authorized"] is False
    assert AUDIT["main_mutated"] is False
