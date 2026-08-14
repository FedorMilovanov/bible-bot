"""Human-agent reviewed promotion boundary for Chapter 5."""

from copy import deepcopy

from .bank import CHAPTER5_STAGING_QUESTIONS
from .research_metadata_v2 import validate_all_research_metadata
from .review_contract_v2 import PRODUCT_REVIEW_RECORDS, validate_full_bank
from .sources import SOURCE_CATALOG

CHAPTER5_REVIEW_QUARANTINE_IDS = frozenset()

# Validate the exact Research metadata independently from the product review
# records, then validate the immutable Research/product handoff before exposing
# any Chapter-5 card to the root pool registry.
validate_all_research_metadata(CHAPTER5_STAGING_QUESTIONS)
validate_full_bank()


def _review_copy(item: dict) -> dict:
    reviewed = deepcopy(item)
    if reviewed["position"] == "project":
        question = str(reviewed["question"])
        if not question.startswith("[Позиция курса]"):
            raise ValueError(f"unlabelled Chapter-5 project card: {reviewed['id']}")
    reviewed["competitive"] = False
    return reviewed


CHAPTER5_REVIEWED_QUESTIONS = [
    _review_copy(item)
    for item in CHAPTER5_STAGING_QUESTIONS
    if item["id"] not in CHAPTER5_REVIEW_QUARANTINE_IDS
]


def reviewed_source_ids(item: dict) -> set[str]:
    """Return only the claim-scoped source subset reviewed for this card."""
    source_ids = {str(source_id) for source_id in item.get("sources", ())}
    unknown = source_ids - set(SOURCE_CATALOG)
    if unknown:
        raise ValueError(f"unknown Chapter-5 reviewed source ids: {sorted(unknown)}")
    return source_ids


__all__ = [
    "CHAPTER5_REVIEWED_QUESTIONS",
    "CHAPTER5_REVIEW_QUARANTINE_IDS",
    "PRODUCT_REVIEW_RECORDS",
    "reviewed_source_ids",
]
