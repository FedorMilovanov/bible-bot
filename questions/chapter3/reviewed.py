"""Reviewed promotion boundary for 1 Peter Chapter 3.

The audited 165-card staging bank crosses into this reviewed aggregate only after
lane integration, Agent-E source audit, cross-lane regression tests, and explicit
coverage checks. Importing this module does not wire Chapter 3 into root product
or ranking pools.
"""

from copy import deepcopy

from . import (
    CHAPTER3_LANE_POOLS,
    CHAPTER3_SOURCE_CATALOGS,
    CHAPTER3_STAGING_QUESTIONS,
)

# No card is silently deleted at this checkpoint. Future editorial removals must
# be explicit IDs here so staging-vs-reviewed drift stays auditable.
CHAPTER3_REVIEW_QUARANTINE_IDS = frozenset()

_LANE_BY_ID: dict[str, str] = {}
for _lane, _items in CHAPTER3_LANE_POOLS.items():
    for _item in _items:
        _qid = str(_item.get("id") or "").strip()
        if not _qid:
            raise ValueError(f"Chapter 3 staging card without id in lane {_lane}")
        if _qid in _LANE_BY_ID:
            raise ValueError(f"Duplicate Chapter 3 staging id: {_qid}")
        _LANE_BY_ID[_qid] = _lane


def _review_copy(item: dict) -> dict:
    """Apply visible review-boundary policy without mutating audited lane objects."""
    reviewed = deepcopy(item)
    if reviewed["position"] == "project":
        question = str(reviewed["question"]).strip()
        if not question.startswith("[Позиция курса]"):
            reviewed["question"] = f"[Позиция курса] {question}"
        reviewed["competitive"] = False
    if reviewed["confidence"] == "contested":
        reviewed["competitive"] = False
    if reviewed["claim_type"] in {"greek", "history", "application"}:
        reviewed["competitive"] = False
    return reviewed


_REVIEWED_SOURCE_ITEMS = [
    item
    for item in CHAPTER3_STAGING_QUESTIONS
    if item["id"] not in CHAPTER3_REVIEW_QUARANTINE_IDS
]

CHAPTER3_REVIEWED_QUESTIONS = [
    _review_copy(item)
    for item in _REVIEWED_SOURCE_ITEMS
]

CHAPTER3_REVIEWED_LANE_BY_ID = {
    item["id"]: _LANE_BY_ID[item["id"]]
    for item in CHAPTER3_REVIEWED_QUESTIONS
}

# Internal candidate list only. Root ranking_policy / COMPETITIVE_POOL do not
# import this set; explicit ranking admission remains a later independent gate.
CHAPTER3_RANKING_CANDIDATE_IDS = frozenset(
    item["id"]
    for item in CHAPTER3_REVIEWED_QUESTIONS
    if item["competitive"] is True
)


def reviewed_source_ids(item: dict) -> set[str]:
    """Return lane-local source IDs used to validate a reviewed card."""
    lane = CHAPTER3_REVIEWED_LANE_BY_ID[item["id"]]
    return set(CHAPTER3_SOURCE_CATALOGS[lane])


__all__ = [
    "CHAPTER3_RANKING_CANDIDATE_IDS",
    "CHAPTER3_REVIEWED_LANE_BY_ID",
    "CHAPTER3_REVIEWED_QUESTIONS",
    "CHAPTER3_REVIEW_QUARANTINE_IDS",
    "reviewed_source_ids",
]
