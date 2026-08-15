"""Reviewed promotion boundary for 1 Peter Chapter 4."""

from __future__ import annotations

from copy import deepcopy

from .authoring import CHAPTER4_STAGING_QUESTIONS

# Explicit product quarantine. A non-empty set is allowed only if excluded from
# the reviewed product aggregate and represented in release audit/dispositions.
CHAPTER4_REVIEW_QUARANTINE_IDS = frozenset()


def _review_copy(item: dict) -> dict:
    reviewed = deepcopy(item)
    if reviewed["position"] == "project":
        question = str(reviewed["question"]).strip()
        if not question.startswith("[Позиция курса]"):
            reviewed["question"] = f"[Позиция курса] {question}"
    if reviewed["id"] == "ch4_tc_001":
        question = str(reviewed["question"]).strip()
        if "SBLGNT" not in question or "ECM/NA28" not in question:
            reviewed["question"] = f"{question} (SBLGNT vs ECM/NA28)"
    reviewed["competitive"] = False
    return reviewed


CHAPTER4_REVIEWED_QUESTIONS = [
    _review_copy(item)
    for item in CHAPTER4_STAGING_QUESTIONS
    if item["id"] not in CHAPTER4_REVIEW_QUARANTINE_IDS
]


def _assert_review_boundary() -> None:
    staged = [
        item
        for item in CHAPTER4_STAGING_QUESTIONS
        if item["id"] not in CHAPTER4_REVIEW_QUARANTINE_IDS
    ]
    staging_ids = [item["id"] for item in CHAPTER4_STAGING_QUESTIONS]
    reviewed_ids = [item["id"] for item in CHAPTER4_REVIEWED_QUESTIONS]
    if len(staging_ids) != len(set(staging_ids)):
        raise ValueError("duplicate Chapter 4 staging ids")
    if len(reviewed_ids) != len(set(reviewed_ids)):
        raise ValueError("duplicate Chapter 4 reviewed ids")
    for source, reviewed in zip(staged, CHAPTER4_REVIEWED_QUESTIONS, strict=True):
        if source is reviewed or source["options"] is reviewed["options"]:
            raise ValueError(f"Chapter 4 reviewed card is not deep-copy isolated: {source['id']}")
        if reviewed["review_record_id"] != source["review_record_id"]:
            raise ValueError(f"Chapter 4 review-record link drifted: {source['id']}")
        if reviewed["position"] == "project" and not reviewed["question"].startswith("[Позиция курса]"):
            raise ValueError(f"Chapter 4 project label is not visible: {reviewed['id']}")
        if reviewed["competitive"] is not False:
            raise ValueError(f"Chapter 4 learning-only card became competitive: {reviewed['id']}")
        private_keys = {
            "research_id", "research_claim_id", "research_effective_claim_digest",
            "sources", "source_ids", "claim_inspection_edge_ids", "inspection_depth",
            "evidence_lane", "research_authority_sha", "reviewer",
        }
        leaked = private_keys.intersection(reviewed)
        if leaked:
            raise ValueError(f"Chapter 4 reviewed runtime card leaks private metadata: {sorted(leaked)}")


_assert_review_boundary()

__all__ = ["CHAPTER4_REVIEWED_QUESTIONS", "CHAPTER4_REVIEW_QUARANTINE_IDS"]
