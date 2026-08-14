from collections import Counter

from questions import (
    BATTLE_POOL,
    CHALLENGE_FALLBACK_POOL,
    CHALLENGE_POOLS,
    COMPETITIVE_POOL,
    POOL_REGISTRY,
)
from questions.chapter4.authoring import CHAPTER4_STAGING_QUESTIONS, answer_position_counts
from questions.chapter4.product_dispositions import CHAPTER4_PRODUCT_DISPOSITIONS
from questions.chapter4.product_sources import SOURCE_CATALOG as CHAPTER4_SOURCE_IDENTITIES
from questions.chapter4.prototype_crosswalk import CHAPTER4_PROTOTYPE_CROSSWALK
from questions.chapter4.ranking_review import W3Q123_RANKING_REVIEW
from questions.chapter4.research_handoff import RESEARCH_HANDOFF_V2
from questions.chapter4.review_contract import (
    validate_all_research_dispositions,
    validate_registry,
)
from questions.chapter4.review_registry import PRODUCT_REVIEW_BY_CARD_ID
from questions.chapter4.reviewed import CHAPTER4_REVIEWED_QUESTIONS
from questions.source_registry import SOURCE_CATALOG


SAFE_RUNTIME_FIELDS = {
    "id",
    "review_record_id",
    "question",
    "options",
    "correct",
    "explanation",
    "verse",
    "topic",
    "domain",
    "claim_type",
    "confidence",
    "position",
    "competitive",
}
PRIVATE_RUNTIME_FIELDS = {
    "research_id",
    "research_claim_id",
    "research_effective_claim_digest",
    "research_authority_sha",
    "research_authority_digest_sha256",
    "sources",
    "source_ids",
    "claim_inspection_edge_ids",
    "inspection_depth",
    "evidence_lane",
    "reviewer",
}


def _ids(items):
    return {str(item.get("id") or "") for item in items}


def _review_by_claim():
    return {
        review["research_claim_id"]: review
        for review in PRODUCT_REVIEW_BY_CARD_ID.values()
    }


def _card_by_claim(reviewed=False):
    items = CHAPTER4_REVIEWED_QUESTIONS if reviewed else CHAPTER4_STAGING_QUESTIONS
    by_id = {item["id"]: item for item in items}
    return {
        review["research_claim_id"]: by_id[review["product_card_id"]]
        for review in PRODUCT_REVIEW_BY_CARD_ID.values()
    }


def test_chapter4_v2_registry_is_complete_and_fail_closed():
    validate_registry()
    assert len(CHAPTER4_STAGING_QUESTIONS) == 52
    assert len(PRODUCT_REVIEW_BY_CARD_ID) == 52
    assert len(_ids(CHAPTER4_STAGING_QUESTIONS)) == 52
    for item in CHAPTER4_STAGING_QUESTIONS:
        assert set(item) == SAFE_RUNTIME_FIELDS
        assert PRIVATE_RUNTIME_FIELDS.isdisjoint(item)
        assert item["review_record_id"]
        assert len(item["options"]) == 4
        assert len({option.strip().casefold() for option in item["options"]}) == 4
        assert item["correct"] in range(4)
        assert item["competitive"] is False


def test_all_52_review_records_have_exact_immutable_research_evidence():
    required = {
        "product_card_id",
        "product_review_record_id",
        "research_repository",
        "research_authority_sha",
        "research_authority_digest_sha256",
        "research_claim_id",
        "research_effective_claim_digest",
        "research_handoff_schema_version",
        "source_ids",
        "claim_inspection_edge_ids",
        "claimed_position",
        "claimed_confidence",
        "claimed_claim_type",
        "product_safe_phrasing_reviewed",
        "overclaim_blacklist_checked",
        "reviewer",
        "review_decision",
        "ranking_considered",
    }
    for review in PRODUCT_REVIEW_BY_CARD_ID.values():
        assert required <= set(review)
        assert "ranking_review_id" not in review
        research = RESEARCH_HANDOFF_V2[review["research_claim_id"]]
        assert review["research_effective_claim_digest"] == research["research_effective_claim_digest"]
        assert review["source_ids"] == research["source_ids"]
        assert review["claim_inspection_edge_ids"] == research["claim_inspection_edge_ids"]
        assert len(review["source_ids"]) == len(review["claim_inspection_edge_ids"])
        assert review["product_safe_phrasing_reviewed"] is True
        assert review["overclaim_blacklist_checked"] is True
        assert review["review_decision"] == "APPROVE_NORMAL_LEARNING_ONLY"
        assert all(review["content_readback"].values())


def test_all_72_research_claims_have_explicit_product_disposition():
    validate_all_research_dispositions()
    assert len(CHAPTER4_PRODUCT_DISPOSITIONS) == 72
    assert set(CHAPTER4_PRODUCT_DISPOSITIONS) == set(RESEARCH_HANDOFF_V2)
    assert Counter(
        row["product_disposition"] for row in CHAPTER4_PRODUCT_DISPOSITIONS.values()
    ) == Counter({"PRODUCT_CARD": 52, "RETAINED_RESEARCH_SUPPORT": 20})
    retained = {
        claim_id
        for claim_id, row in CHAPTER4_PRODUCT_DISPOSITIONS.items()
        if row["product_disposition"] == "RETAINED_RESEARCH_SUPPORT"
    }
    assert retained == {
        "w3q_007", "w3q_036", "w3q_043", "w3q_044", "w3q_045", "w3q_092",
        "w3q_098", "w3q_099", "w3q_101", "w3q_104", "w3q_114", "w3q_115",
        "w3q_123", "w3q_124", "w3q_129", "w3q_131", "w3q_132", "w3q_138",
        "w3q_139", "w3q_140",
    }
    assert all(CHAPTER4_PRODUCT_DISPOSITIONS[claim_id]["reason"] for claim_id in retained)


def test_all_32_agent_e_prototypes_are_reconciled_without_rehabilitation():
    assert len(CHAPTER4_PROTOTYPE_CROSSWALK) == 32
    assert Counter(
        row["agent_e_classification"]
        for row in CHAPTER4_PROTOTYPE_CROSSWALK.values()
    ) == Counter(
        {
            "SAFE_TEMPLATE": 13,
            "NEEDS_REWRITE": 10,
            "NONCOMPETITIVE_ONLY": 6,
            "REJECT_AS_PRODUCT_TEMPLATE": 3,
        }
    )
    for prototype_id in ("w3mcq_003", "w3mcq_037", "w3mcq_047"):
        row = CHAPTER4_PROTOTYPE_CROSSWALK[prototype_id]
        assert row["agent_e_classification"] == "REJECT_AS_PRODUCT_TEMPLATE"
        assert row["agent_e_reasons"] == ("REFERENCE_DRIFT",)
        assert row["product_resolution"].startswith("INDEPENDENT_PRODUCT_REWRITE")
        assert row["prototype_is_product_authority"] is False
    for row in CHAPTER4_PROTOTYPE_CROSSWALK.values():
        if row["agent_e_classification"] != "SAFE_TEMPLATE":
            assert row["mechanical_copy_forbidden"] is True


def test_w3q123_is_closed_with_no_ranking_admission():
    assert RESEARCH_HANDOFF_V2["w3q_123"]["ranking_discrepancy_candidate"] is True
    assert W3Q123_RANKING_REVIEW["research_claim_id"] == "w3q_123"
    assert W3Q123_RANKING_REVIEW["product_ranking_decision"] == "NO_RANKING_ADMISSION"
    assert W3Q123_RANKING_REVIEW["competitive_pool_admission"] is False
    assert W3Q123_RANKING_REVIEW["battle_admission"] is False
    assert W3Q123_RANKING_REVIEW["challenge_admission"] is False
    assert CHAPTER4_PRODUCT_DISPOSITIONS["w3q_123"]["ranking_review_id"] == W3Q123_RANKING_REVIEW["ranking_review_id"]


def test_chapter4_answer_positions_and_deep_copy_are_stable():
    assert answer_position_counts() == {0: 13, 1: 13, 2: 13, 3: 13}
    staged = {item["id"]: item for item in CHAPTER4_STAGING_QUESTIONS}
    reviewed = {item["id"]: item for item in CHAPTER4_REVIEWED_QUESTIONS}
    assert staged.keys() == reviewed.keys()
    for qid in staged:
        assert staged[qid] is not reviewed[qid]
        assert staged[qid]["options"] is not reviewed[qid]["options"]
        assert staged[qid]["review_record_id"] == reviewed[qid]["review_record_id"]
        assert reviewed[qid]["competitive"] is False


def test_sensitive_epistemic_boundaries_are_explicit_and_fail_closed():
    reviewed = _card_by_claim(reviewed=True)
    course_4_6 = reviewed["w3q_013"]
    neutral_4_6 = reviewed["w3q_012"]
    morphology_4_6 = reviewed["w3q_011"]
    malachi = reviewed["w3q_038"]
    ecm_4_14 = reviewed["w3q_031"]
    sinaiticus = reviewed["w3q_137"]
    edition_4_16 = reviewed["w3q_121"]

    assert course_4_6["position"] == "project"
    assert course_4_6["question"].startswith("[Позиция курса]")
    assert neutral_4_6["position"] == "neutral"
    assert neutral_4_6["confidence"] == "contested"
    assert "хронологи" in morphology_4_6["explanation"]
    assert "мест" in morphology_4_6["explanation"]
    assert "адресат" in morphology_4_6["explanation"]
    assert "formal/exclusive quotation" in malachi["explanation"]
    assert "manuscript unanimity" in ecm_4_14["explanation"]
    assert "один свидетель" in sinaiticus["explanation"]
    assert "SBLGNT" in edition_4_16["question"]
    assert "ECM/NA28" in edition_4_16["question"]
    assert all(
        card["competitive"] is False
        for claim_id, card in reviewed.items()
        if RESEARCH_HANDOFF_V2[claim_id]["claim_type"] in {"text", "interpretation"}
        and card["domain"] == "textual_criticism"
    )


def test_every_v2_research_source_identity_resolves_without_global_depth_upgrade():
    all_source_ids = {
        source_id
        for research in RESEARCH_HANDOFF_V2.values()
        for source_id in research["source_ids"]
    }
    assert all_source_ids <= set(SOURCE_CATALOG)
    forbidden_depth_keys = {
        "evidence_status",
        "inspection_level",
        "inspection_scope",
        "actual_inspection_depth",
        "strongest_evidence_status",
        "claim_inspection_edge_ids",
    }
    for metadata in CHAPTER4_SOURCE_IDENTITIES.values():
        assert metadata["source_identity_only"] is True
        assert forbidden_depth_keys.isdisjoint(metadata)


def test_chapter4_has_zero_paths_into_competitive_surfaces():
    chapter4_ids = _ids(POOL_REGISTRY["chapter4"])
    challenge_ids = set().union(*(_ids(pool) for pool in CHALLENGE_POOLS.values()))
    assert chapter4_ids
    assert chapter4_ids.isdisjoint(_ids(POOL_REGISTRY["random_all"]))
    assert chapter4_ids.isdisjoint(_ids(COMPETITIVE_POOL))
    assert chapter4_ids.isdisjoint(_ids(BATTLE_POOL))
    assert chapter4_ids.isdisjoint(challenge_ids)
    assert chapter4_ids.isdisjoint(_ids(CHALLENGE_FALLBACK_POOL))
