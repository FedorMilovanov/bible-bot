"""Independent Chapter 5 ranking audit: zero gameplay admission."""

from .reviewed import CHAPTER5_REVIEWED_QUESTIONS

CHAPTER5_RANKING_READY_IDS = frozenset()
CHAPTER5_RANKING_HOLD_IDS = frozenset(item["id"] for item in CHAPTER5_REVIEWED_QUESTIONS)
RANKING_AUDIT = {
    "reviewed": len(CHAPTER5_REVIEWED_QUESTIONS),
    "ready": 0,
    "hold": len(CHAPTER5_RANKING_HOLD_IDS),
    "reason": "Research Wave 3 reports COMPETITIVE_CANDIDATES=0; no later gameplay authority exists.",
    "battle_admission": 0,
    "challenge_admission": 0,
}

__all__ = ["CHAPTER5_RANKING_HOLD_IDS", "CHAPTER5_RANKING_READY_IDS", "RANKING_AUDIT"]
