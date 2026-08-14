"""Reviewed promotion boundary for 1 Peter Chapter 4."""

from __future__ import annotations

from copy import deepcopy

from .authoring import CHAPTER4_STAGING_QUESTIONS

# Explicit and auditable. Research HOLD=0 does not imply this set must be empty;
# product review owns this boundary independently.
CHAPTER4_REVIEW_QUARANTINE_IDS = frozenset()


def _review_copy(item: dict) -> dict:
    reviewed = deepcopy(item)
    if reviewed["position"] == "project":
        question = str(reviewed["question"]).strip()
        if not question.startswith("[Позиция курса]"):
            reviewed["question"] = f"[Позиция курса] {question}"
    # Chapter 4 is deliberately learning-only in this branch. Research reported
    # zero competitive candidates and product ranking audit must fail closed.
    reviewed["competitive"] = False
    return reviewed


CHAPTER4_REVIEWED_QUESTIONS = [
    _review_copy(item)
    for item in CHAPTER4_STAGING_QUESTIONS
    if item["id"] not in CHAPTER4_REVIEW_QUARANTINE_IDS
]


def _assert_review_boundary() -> None:
    staging_ids = [item["id"] for item in CHAPTER4_STAGING_QUESTIONS]
    reviewed_ids = [item["id"] for item in CHAPTER4_REVIEWED_QUESTIONS]
    if len(staging_ids) != len(set(staging_ids)):
        raise ValueError("duplicate Chapter 4 staging ids")
    if len(reviewed_ids) != len(set(reviewed_ids)):
        raise ValueError("duplicate Chapter 4 reviewed ids")
    for source, reviewed in zip(
        [item for item in CHAPTER4_STAGING_QUESTIONS if item["id"] not in CHAPTER4_REVIEW_QUARANTINE_IDS],
        CHAPTER4_REVIEWED_QUESTIONS,
        strict=True,
    ):
        if source is reviewed or source["options"] is reviewed["options"] or source["sources"] is reviewed["sources"]:
            raise ValueError(f"Chapter 4 reviewed card is not isolated: {source['id']}")
        if reviewed["position"] == "project" and not reviewed["question"].startswith("[Позиция курса]"):
            raise ValueError(f"Chapter 4 project label is not visible: {reviewed['id']}")
        if reviewed["competitive"] is not False:
            raise ValueError(f"Chapter 4 learning-only card became competitive: {reviewed['id']}")


_assert_review_boundary()

__all__ = ["CHAPTER4_REVIEWED_QUESTIONS", "CHAPTER4_REVIEW_QUARANTINE_IDS"]
