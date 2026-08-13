"""Explicit ranking eligibility policy and review registration."""
from __future__ import annotations

from .content_truth import SOURCE_CATALOG
from .content_truth_review import REVIEW_OVERRIDES
from .geography_review import GEOGRAPHY_OVERRIDES
from .nero_review import ADDITIONAL_SOURCE_CATALOG, NERO_OVERRIDES

REVIEW_OVERRIDES.update(GEOGRAPHY_OVERRIDES)
REVIEW_OVERRIDES.update(NERO_OVERRIDES)
SOURCE_CATALOG.update(ADDITIONAL_SOURCE_CATALOG)

SOURCE_REVIEWED_RANKING_IDS = frozenset(
    {"easy_12", "med_02", "hard_02", "hard_12"}
)

def ranking_eligible(question: dict) -> bool:
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
