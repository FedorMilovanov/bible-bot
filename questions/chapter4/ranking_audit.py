"""Fail-closed Chapter 4 ranking audit.

Research authority ended with COMPETITIVE_CANDIDATES=0. Product authoring does
not manufacture a Battle/Challenge candidate merely to obtain a non-zero count.
"""

from __future__ import annotations

from .reviewed import CHAPTER4_REVIEWED_QUESTIONS

CHAPTER4_RANKING_READY_IDS = frozenset()
CHAPTER4_RANKING_AUDIT = {
    "status": "LEARNING_ONLY_FAIL_CLOSED",
    "reviewed_count": len(CHAPTER4_REVIEWED_QUESTIONS),
    "ready_count": 0,
    "battle_count": 0,
    "challenge_count": 0,
    "reason": (
        "Research snapshot has zero competitive candidates. This product branch "
        "keeps every Chapter 4 card learning-only; any future ranking admission "
        "requires a separate explicit authority step and exact source-depth audit."
    ),
    "rules": [
        "PROJECT_NE_NEUTRAL_FACT",
        "CONTESTED_NE_RANKING",
        "MORPHOLOGY_NE_EXEGESIS",
        "HISTORY_RECONSTRUCTION_NE_OBJECTIVE_TEXT_FACT",
        "ZERO_RESEARCH_HOLDS_NE_PRODUCTION_READY",
        "GREEN_VALIDATOR_NE_PUBLICATION_APPROVAL",
    ],
}

if any(item.get("competitive") is True for item in CHAPTER4_REVIEWED_QUESTIONS):
    raise ValueError("Chapter 4 ranking audit is fail-closed but a reviewed card is competitive")

__all__ = ["CHAPTER4_RANKING_AUDIT", "CHAPTER4_RANKING_READY_IDS"]
