"""Canonical effective Research metadata for Chapter 5 product validation."""
from __future__ import annotations

from ..research_handoff_v2 import CHAPTER5_RESEARCH_HANDOFF_V2
from .review_contract_v2 import CHAPTER5_CANDIDATE_IDS


def expected_claim_type(candidate_id: str) -> str:
    return str(CHAPTER5_RESEARCH_HANDOFF_V2[candidate_id]["claim_type"])


def expected_confidence(candidate_id: str) -> str:
    return str(CHAPTER5_RESEARCH_HANDOFF_V2[candidate_id]["confidence"])


def expected_position(candidate_id: str) -> str:
    return str(CHAPTER5_RESEARCH_HANDOFF_V2[candidate_id]["position"])


APPLICATION_PROJECT_IDS = frozenset(
    candidate_id
    for candidate_id, record in CHAPTER5_RESEARCH_HANDOFF_V2.items()
    if record["claim_type"] == "application" and record["position"] == "project"
)
HISTORY_IDS = frozenset(
    candidate_id
    for candidate_id, record in CHAPTER5_RESEARCH_HANDOFF_V2.items()
    if record["claim_type"] == "history"
)
GREEK_IDS = frozenset(
    candidate_id
    for candidate_id, record in CHAPTER5_RESEARCH_HANDOFF_V2.items()
    if record["claim_type"] == "greek"
)
INTERPRETATION_IDS = frozenset(
    candidate_id
    for candidate_id, record in CHAPTER5_RESEARCH_HANDOFF_V2.items()
    if record["claim_type"] == "interpretation"
)
CONTESTED_CONFIDENCE_IDS = frozenset(
    candidate_id
    for candidate_id, record in CHAPTER5_RESEARCH_HANDOFF_V2.items()
    if record["confidence"] == "contested"
)
MEDIUM_CONFIDENCE_IDS = frozenset(
    candidate_id
    for candidate_id, record in CHAPTER5_RESEARCH_HANDOFF_V2.items()
    if record["confidence"] == "medium"
)


def validate_research_metadata(card: dict) -> None:
    candidate_id = str(card["research_candidate_id"])
    research = CHAPTER5_RESEARCH_HANDOFF_V2[candidate_id]
    if str(card["position"]) != research["position"]:
        raise ValueError(
            f"Chapter-5 position drift for {candidate_id}: "
            f"actual={card['position']}, expected={research['position']}"
        )
    confidence_rank = {"contested": 0, "medium": 1, "high": 2}
    if confidence_rank[str(card["confidence"])] > confidence_rank[str(research["confidence"])]:
        raise ValueError(
            f"Chapter-5 confidence strengthened for {candidate_id}: "
            f"actual={card['confidence']}, expected<={research['confidence']}"
        )
    if str(card["claim_type"]) != research["claim_type"]:
        raise ValueError(
            f"Chapter-5 claim-type drift for {candidate_id}: "
            f"actual={card['claim_type']}, expected={research['claim_type']}"
        )


def validate_all_research_metadata(cards: list[dict]) -> None:
    if tuple(str(card["research_candidate_id"]) for card in cards) != CHAPTER5_CANDIDATE_IDS:
        raise ValueError("Chapter-5 Research metadata validation received wrong claim set/order")
    for card in cards:
        validate_research_metadata(card)


__all__ = [
    "APPLICATION_PROJECT_IDS", "CONTESTED_CONFIDENCE_IDS", "GREEK_IDS", "HISTORY_IDS",
    "INTERPRETATION_IDS", "MEDIUM_CONFIDENCE_IDS", "expected_claim_type",
    "expected_confidence", "expected_position", "validate_all_research_metadata",
    "validate_research_metadata",
]
