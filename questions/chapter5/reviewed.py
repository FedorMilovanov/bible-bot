"""Human-agent reviewed promotion boundary for Chapter 5."""

from copy import deepcopy

from .bank import CHAPTER5_STAGING_QUESTIONS
from .sources import SOURCE_CATALOG

CHAPTER5_REVIEW_QUARANTINE_IDS = frozenset()


def _review_copy(item: dict) -> dict:
    reviewed = deepcopy(item)
    if reviewed["position"] == "project":
        question = str(reviewed["question"]).strip()
        if not question.startswith("[Позиция курса]"):
            reviewed["question"] = f"[Позиция курса] {question}"
    reviewed["competitive"] = False
    return reviewed


CHAPTER5_REVIEWED_QUESTIONS = [
    _review_copy(item)
    for item in CHAPTER5_STAGING_QUESTIONS
    if item["id"] not in CHAPTER5_REVIEW_QUARANTINE_IDS
]


def reviewed_source_ids(item: dict) -> set[str]:
    """Return source identities available to this Chapter-5 owner lane."""
    return set(SOURCE_CATALOG)


__all__ = ["CHAPTER5_REVIEWED_QUESTIONS", "CHAPTER5_REVIEW_QUARANTINE_IDS", "reviewed_source_ids"]
