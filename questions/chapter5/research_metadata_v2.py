"""Independent effective Research metadata pins for Chapter 5.

These sets are transcribed from the exact Research authority at
0142430af8ba80f28e0fd9cde669d32611a1d2af after the Wave3n override layer.
They are deliberately independent of product-bank metadata so a simultaneous
card/review mutation cannot redefine the Research contract.
"""
from __future__ import annotations

from .review_contract_v2 import CHAPTER5_CANDIDATE_IDS

APPLICATION_PROJECT_IDS = frozenset({
    "w3q_054", "w3q_063", "w3q_071", "w3q_083", "w3q_087",
    "w3q_105", "w3q_106", "w3q_107", "w3q_108", "w3q_109", "w3q_110", "w3q_111", "w3q_112",
})
HISTORY_IDS = frozenset({"w3q_080", "w3q_117", "w3q_118", "w3q_119", "w3q_120"})
GREEK_IDS = frozenset({
    "w3q_049", "w3q_056", "w3q_061", "w3q_065", "w3q_068", "w3q_069",
    "w3q_076", "w3q_085", "w3q_096", "w3q_133", "w3q_134", "w3q_135", "w3q_136",
})
INTERPRETATION_IDS = frozenset({
    "w3q_047", "w3q_053", "w3q_059", "w3q_062", "w3q_073", "w3q_074",
    "w3q_077", "w3q_078", "w3q_079", "w3q_084", "w3q_088", "w3q_089", "w3q_128",
})

CONTESTED_CONFIDENCE_IDS = frozenset({
    "w3q_047", "w3q_073", "w3q_077", "w3q_078", "w3q_079", "w3q_080", "w3q_118", "w3q_119",
})
MEDIUM_CONFIDENCE_IDS = frozenset({
    "w3q_053", "w3q_054", "w3q_062", "w3q_063", "w3q_071", "w3q_074", "w3q_083", "w3q_084",
    "w3q_087", "w3q_088", "w3q_089",
    "w3q_105", "w3q_106", "w3q_107", "w3q_108", "w3q_109", "w3q_110", "w3q_111", "w3q_112",
    "w3q_117", "w3q_120", "w3q_125", "w3q_126", "w3q_127", "w3q_128",
    "w3q_133", "w3q_134", "w3q_135", "w3q_136",
})


def expected_claim_type(candidate_id: str) -> str:
    if candidate_id in APPLICATION_PROJECT_IDS:
        return "application"
    if candidate_id in HISTORY_IDS:
        return "history"
    if candidate_id in GREEK_IDS:
        return "greek"
    if candidate_id in INTERPRETATION_IDS:
        return "interpretation"
    if candidate_id in CHAPTER5_CANDIDATE_IDS:
        return "text"
    raise KeyError(candidate_id)


def expected_confidence(candidate_id: str) -> str:
    if candidate_id in CONTESTED_CONFIDENCE_IDS:
        return "contested"
    if candidate_id in MEDIUM_CONFIDENCE_IDS:
        return "medium"
    if candidate_id in CHAPTER5_CANDIDATE_IDS:
        return "high"
    raise KeyError(candidate_id)


def expected_position(candidate_id: str) -> str:
    if candidate_id not in CHAPTER5_CANDIDATE_IDS:
        raise KeyError(candidate_id)
    return "project" if candidate_id in APPLICATION_PROJECT_IDS else "neutral"


def validate_research_metadata(card: dict) -> None:
    candidate_id = str(card["research_candidate_id"])
    expected = (
        expected_claim_type(candidate_id),
        expected_confidence(candidate_id),
        expected_position(candidate_id),
    )
    actual = (str(card["claim_type"]), str(card["confidence"]), str(card["position"]))
    if actual != expected:
        raise ValueError(
            f"Chapter-5 Research metadata drift for {candidate_id}: actual={actual}, expected={expected}"
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
