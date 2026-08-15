"""Fail-closed Chapter 4 ranking audit with explicit w3q_123 reconciliation."""

from __future__ import annotations

from .ranking_review import W3Q123_RANKING_REVIEW
from .reviewed import CHAPTER4_REVIEWED_QUESTIONS

CHAPTER4_RANKING_READY_IDS = frozenset()
CHAPTER4_RANKING_AUDIT = {
    "schema_version": 2,
    "status": "LEARNING_ONLY_FAIL_CLOSED",
    "reviewed_count": len(CHAPTER4_REVIEWED_QUESTIONS),
    "ready_count": 0,
    "battle_count": 0,
    "challenge_count": 0,
    "ranking_discrepancy_count": 1,
    "ranking_reviews": {"w3q_123": W3Q123_RANKING_REVIEW},
    "reason": (
        "Research v2 contains no competitive Chapter-4 authority. The single "
        "mechanical ranking discrepancy w3q_123 was independently reviewed and "
        "closed as NO_RANKING_ADMISSION; no gameplay surface is enlarged."
    ),
    "rules": [
        "PROJECT_NE_NEUTRAL_FACT",
        "CONTESTED_NE_RANKING",
        "MORPHOLOGY_NE_EXEGESIS",
        "TEXTUAL_CRITICISM_REMAINS_NONCOMPETITIVE",
        "NAMED_WITNESS_NE_AUSGANGSTEXT",
        "ECM_DECISION_NE_MANUSCRIPT_UNANIMITY",
        "ZERO_RESEARCH_HOLDS_NE_RANKING_AUTHORITY",
        "GREEN_VALIDATOR_NE_PUBLICATION_APPROVAL",
    ],
}

if any(item.get("competitive") is True for item in CHAPTER4_REVIEWED_QUESTIONS):
    raise ValueError("Chapter 4 ranking audit is fail-closed but a reviewed card is competitive")
if W3Q123_RANKING_REVIEW["product_ranking_decision"] != "NO_RANKING_ADMISSION":
    raise ValueError("w3q_123 ranking discrepancy was not closed fail-closed")

__all__ = ["CHAPTER4_RANKING_AUDIT", "CHAPTER4_RANKING_READY_IDS"]
