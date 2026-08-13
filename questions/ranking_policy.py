"""Explicit ranking eligibility policy for canonical questions."""
from __future__ import annotations


SOURCE_REVIEWED_RANKING_IDS = frozenset(
    {
        "easy_12",
        "med_02",
        "hard_02",
        "hard_12",
    }
)


def ranking_eligible(question: dict) -> bool:
    """Return whether a canonical question may affect PvP/Challenge ranking."""
    qid = str(question.get("id") or "").strip()
    if qid in SOURCE_REVIEWED_RANKING_IDS:
        return True
    return bool(
        question.get("competitive") is True
        and question.get("confidence") == "high"
        and question.get("position") == "neutral"
        and question.get("claim_type") == "text"
        and question.get("sources")
    )


__all__ = ["SOURCE_REVIEWED_RANKING_IDS", "ranking_eligible"]
