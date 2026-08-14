"""Production lifecycle exports for 1 Peter Chapter 4."""

from .authoring import CHAPTER4_STAGING_QUESTIONS, answer_position_counts
from .second_pass_revisions import apply_second_pass_card_revisions

# Required second adversarial content pass is sequenced after first-green exact
# head 2f9ae1cb... and before the reviewed/ranking product boundary is built.
apply_second_pass_card_revisions(CHAPTER4_STAGING_QUESTIONS)

# Preserve compatibility for modules that import questions.chapter4.review_registry:
# changed cards now resolve to their post-green immutable record IDs/digests.
from . import review_registry as _review_registry  # noqa: E402
from .final_review_registry import (  # noqa: E402
    PRODUCT_REVIEW_BY_CARD_ID as _FINAL_REVIEW_BY_CARD_ID,
    PRODUCT_REVIEW_REGISTRY as _FINAL_REVIEW_REGISTRY,
)

_review_registry.PRODUCT_REVIEW_BY_CARD_ID = _FINAL_REVIEW_BY_CARD_ID
_review_registry.PRODUCT_REVIEW_REGISTRY = _FINAL_REVIEW_REGISTRY

from .ranking_audit import CHAPTER4_RANKING_AUDIT, CHAPTER4_RANKING_READY_IDS  # noqa: E402
from .reviewed import CHAPTER4_REVIEWED_QUESTIONS, CHAPTER4_REVIEW_QUARANTINE_IDS  # noqa: E402

CHAPTER4_PRODUCT_QUESTIONS = list(CHAPTER4_REVIEWED_QUESTIONS)

__all__ = [
    "CHAPTER4_PRODUCT_QUESTIONS",
    "CHAPTER4_RANKING_AUDIT",
    "CHAPTER4_RANKING_READY_IDS",
    "CHAPTER4_REVIEWED_QUESTIONS",
    "CHAPTER4_REVIEW_QUARANTINE_IDS",
    "CHAPTER4_STAGING_QUESTIONS",
    "answer_position_counts",
]
