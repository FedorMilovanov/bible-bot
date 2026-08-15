from copy import deepcopy

import pytest

import questions
from questions.chapter4.authoring import CHAPTER4_STAGING_QUESTIONS
from questions.chapter4.release_guard import validate_gameplay_exclusion
from questions.chapter4.review_contract import validate_review_binding
from questions.chapter4.review_registry import PRODUCT_REVIEW_BY_CARD_ID


def _card_and_review_by_claim(claim_id: str):
    for review in PRODUCT_REVIEW_BY_CARD_ID.values():
        if review["research_claim_id"] == claim_id:
            card = next(
                item
                for item in CHAPTER4_STAGING_QUESTIONS
                if item["id"] == review["product_card_id"]
            )
            return deepcopy(card), dict(review)
    raise AssertionError(f"claim not mapped to product card: {claim_id}")


def test_rejects_forged_research_claim_id():
    card, review = _card_and_review_by_claim("w3q_001")
    review["research_claim_id"] = "w3q_999"
    with pytest.raises(ValueError, match="forged or unknown Research claim id"):
        validate_review_binding(card, review)


def test_rejects_old_claim_id_with_wrong_digest():
    card, review = _card_and_review_by_claim("w3q_001")
    review["research_effective_claim_digest"] = "0" * 64
    with pytest.raises(ValueError, match="effective-claim digest mismatch"):
        validate_review_binding(card, review)


def test_rejects_swapped_claim_inspection_edge_id():
    card, review = _card_and_review_by_claim("w3q_014")
    edges = list(review["claim_inspection_edge_ids"])
    edges[0], edges[1] = edges[1], edges[0]
    review["claim_inspection_edge_ids"] = tuple(edges)
    with pytest.raises(ValueError, match="claim-inspection edge mismatch"):
        validate_review_binding(card, review)


def test_rejects_source_identity_without_edge():
    card, review = _card_and_review_by_claim("w3q_014")
    review["claim_inspection_edge_ids"] = review["claim_inspection_edge_ids"][:-1]
    with pytest.raises(ValueError, match="cardinality mismatch"):
        validate_review_binding(card, review)


def test_rejects_project_to_neutral_promotion():
    card, review = _card_and_review_by_claim("w3q_013")
    card["position"] = "neutral"
    review["claimed_position"] = "neutral"
    with pytest.raises(ValueError, match="project Research claim promoted to neutral"):
        validate_review_binding(card, review)


def test_rejects_confidence_promotion():
    card, review = _card_and_review_by_claim("w3q_012")
    card["confidence"] = "high"
    review["claimed_confidence"] = "high"
    with pytest.raises(ValueError, match="confidence promoted"):
        validate_review_binding(card, review)


def test_rejects_interpretation_to_text_promotion():
    card, review = _card_and_review_by_claim("w3q_012")
    card["claim_type"] = "text"
    review["claimed_claim_type"] = "text"
    with pytest.raises(ValueError, match="claim type changed"):
        validate_review_binding(card, review)


def test_rejects_competitive_flag_enablement():
    card, review = _card_and_review_by_claim("w3q_001")
    card["competitive"] = True
    with pytest.raises(ValueError, match="competitive flag enabled"):
        validate_review_binding(card, review)


@pytest.mark.parametrize(
    "surface",
    ["random_all", "competitive", "battle", "challenge", "challenge_fallback"],
)
def test_release_guard_rejects_chapter4_in_any_gameplay_surface(surface):
    malicious = [CHAPTER4_STAGING_QUESTIONS[0]]
    kwargs = {
        "random_all": list(questions.POOL_REGISTRY["random_all"]),
        "competitive_pool": list(questions.COMPETITIVE_POOL),
        "battle_pool": list(questions.BATTLE_POOL),
        "challenge_pools": {
            key: list(pool) for key, pool in questions.CHALLENGE_POOLS.items()
        },
        "challenge_fallback": list(questions.CHALLENGE_FALLBACK_POOL),
    }
    if surface == "random_all":
        kwargs["random_all"] += malicious
    elif surface == "competitive":
        kwargs["competitive_pool"] += malicious
    elif surface == "battle":
        kwargs["battle_pool"] += malicious
    elif surface == "challenge":
        kwargs["challenge_pools"]["easy"] += malicious
    else:
        kwargs["challenge_fallback"] += malicious

    with pytest.raises(ValueError, match="Chapter 4 leaked"):
        validate_gameplay_exclusion(CHAPTER4_STAGING_QUESTIONS, **kwargs)
