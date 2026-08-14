"""Fail-closed Chapter 4 product/research review-contract validation."""

from __future__ import annotations

import re
from collections import Counter

from .authoring import CHAPTER4_STAGING_QUESTIONS
from .product_dispositions import CHAPTER4_PRODUCT_DISPOSITIONS
from .research_handoff import (
    RESEARCH_AUTHORITY_DIGEST_SHA256,
    RESEARCH_AUTHORITY_SHA,
    RESEARCH_HANDOFF_SCHEMA_VERSION,
    RESEARCH_HANDOFF_V2,
    RESEARCH_REPOSITORY,
)
from .review_registry import PRODUCT_REVIEW_BY_CARD_ID, product_card_content_digest

_PRIVATE_RUNTIME_KEYS = frozenset({
    "research_id", "research_claim_id", "research_effective_claim_digest",
    "research_authority_sha", "research_authority_digest_sha256",
    "sources", "source_ids", "claim_inspection_edge_ids", "inspection_depth",
    "inspection_scope", "evidence_lane", "source_evidence", "reviewer",
    "review_decision", "field_provenance", "provenance",
})
_CONFIDENCE = {"contested": 0, "medium": 1, "high": 2}
_ABSOLUTE_DISTRACTOR = re.compile(
    r"\b(?:только|всегда|никогда|обязательно|автоматически|несомненно|"
    r"исключительно|каждый|полностью|любой|любая|любое|единствен\w*)\b",
    re.IGNORECASE,
)


def _fail(condition: bool, message: str) -> None:
    if condition:
        raise ValueError(message)


def validate_review_binding(
    card: dict,
    review: dict,
    *,
    research_by_id: dict = RESEARCH_HANDOFF_V2,
) -> None:
    """Reject any product/research binding that is not exact and fail-closed."""
    private = _PRIVATE_RUNTIME_KEYS.intersection(card)
    _fail(bool(private), f"private Research/review metadata leaked into runtime card: {sorted(private)}")

    _fail(card.get("review_record_id") != review.get("product_review_record_id"), "review record link mismatch")
    _fail(card.get("id") != review.get("product_card_id"), "product card id mismatch")
    _fail(
        review.get("product_card_content_digest_sha256") != product_card_content_digest(card),
        "product card content digest mismatch",
    )
    _fail(review.get("research_repository") != RESEARCH_REPOSITORY, "Research repository mismatch")
    _fail(review.get("research_authority_sha") != RESEARCH_AUTHORITY_SHA, "Research SHA mismatch")
    _fail(
        review.get("research_authority_digest_sha256") != RESEARCH_AUTHORITY_DIGEST_SHA256,
        "Research authority digest mismatch",
    )
    _fail(
        review.get("research_handoff_schema_version") != RESEARCH_HANDOFF_SCHEMA_VERSION,
        "Research handoff schema mismatch",
    )

    claim_id = review.get("research_claim_id")
    _fail(claim_id not in research_by_id, f"forged or unknown Research claim id: {claim_id}")
    research = research_by_id[claim_id]
    _fail(
        review.get("research_effective_claim_digest") != research["research_effective_claim_digest"],
        "Research effective-claim digest mismatch",
    )

    sources = tuple(review.get("source_ids") or ())
    edges = tuple(review.get("claim_inspection_edge_ids") or ())
    _fail(not sources or not edges, "source identity without exact claim-inspection edge")
    _fail(len(sources) != len(edges), "source/inspection-edge cardinality mismatch")
    _fail(sources != tuple(research["source_ids"]), "Research source identity set/order mismatch")
    _fail(edges != tuple(research["claim_inspection_edge_ids"]), "Research claim-inspection edge mismatch")

    claimed_position = review.get("claimed_position")
    claimed_confidence = review.get("claimed_confidence")
    claimed_claim_type = review.get("claimed_claim_type")
    _fail(claimed_position != card.get("position"), "product review/card position mismatch")
    _fail(claimed_claim_type != card.get("claim_type"), "product review/card claim type mismatch")
    _fail(claimed_confidence != card.get("confidence"), "product review/card confidence mismatch")

    if research["position"] == "project":
        _fail(claimed_position != "project", "project Research claim promoted to neutral")
    _fail(
        _CONFIDENCE.get(claimed_confidence, -1) > _CONFIDENCE.get(research["confidence"], -1),
        "product confidence promoted above Research authority",
    )
    _fail(claimed_claim_type != research["claim_type"], "Research claim type changed in product review")

    _fail(card.get("competitive") is not False, "Chapter 4 card competitive flag enabled")
    _fail(review.get("product_safe_phrasing_reviewed") is not True, "safe phrasing review missing")
    _fail(review.get("overclaim_blacklist_checked") is not True, "overclaim blacklist review missing")
    _fail(review.get("review_decision") != "APPROVE_NORMAL_LEARNING_ONLY", "invalid product review decision")
    reviewer = review.get("reviewer") or {}
    _fail(not reviewer.get("reviewer_id") or not reviewer.get("reviewer_role"), "explicit reviewer record missing")
    _fail(review.get("ranking_considered") is not False, "card-level ranking review unexpectedly enabled")
    _fail("ranking_review_id" in review, "ranking_review_id present without ranking consideration")

    options = card.get("options")
    _fail(not isinstance(options, list) or len(options) != 4, "product card must have exactly four options")
    _fail(len({str(option).strip().casefold() for option in options}) != 4, "product options are not distinct")
    correct = card.get("correct")
    _fail(not isinstance(correct, int) or correct not in range(4), "invalid keyed answer")
    for index, option in enumerate(options):
        if index != correct:
            _fail(
                bool(_ABSOLUTE_DISTRACTOR.search(str(option))),
                f"absolute-certainty distractor survived product review: {card.get('id')}[{index}]",
            )


def validate_registry(cards: list[dict] | None = None) -> None:
    cards = list(cards or CHAPTER4_STAGING_QUESTIONS)
    _fail(len(cards) != 52, "Chapter 4 staging card count must be 52")
    _fail(len({card["id"] for card in cards}) != 52, "duplicate Chapter 4 product card id")
    _fail(len({card["review_record_id"] for card in cards}) != 52, "duplicate Chapter 4 review-record link")
    _fail(set(PRODUCT_REVIEW_BY_CARD_ID) != {card["id"] for card in cards}, "review registry/card set mismatch")
    for card in cards:
        validate_review_binding(card, PRODUCT_REVIEW_BY_CARD_ID[card["id"]])


def validate_all_research_dispositions() -> None:
    _fail(set(CHAPTER4_PRODUCT_DISPOSITIONS) != set(RESEARCH_HANDOFF_V2), "72-claim disposition coverage drift")
    counts = Counter(row["product_disposition"] for row in CHAPTER4_PRODUCT_DISPOSITIONS.values())
    _fail(counts != Counter({"PRODUCT_CARD": 52, "RETAINED_RESEARCH_SUPPORT": 20}), "Chapter 4 disposition counts drift")
    for claim_id, disposition in CHAPTER4_PRODUCT_DISPOSITIONS.items():
        _fail(
            disposition["research_effective_claim_digest"]
            != RESEARCH_HANDOFF_V2[claim_id]["research_effective_claim_digest"],
            f"disposition digest mismatch: {claim_id}",
        )


__all__ = [
    "validate_review_binding",
    "validate_registry",
    "validate_all_research_dispositions",
]
