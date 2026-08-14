"""Production lifecycle exports for 1 Peter Chapter 4."""

from .authoring import CHAPTER4_STAGING_QUESTIONS, answer_position_counts
from .second_pass_revisions import apply_second_pass_card_revisions

# Second adversarial content pass is a sequenced, auditable product revision
# after first-green exact head 2f9ae1cb...; apply before reviewed/ranking imports.
apply_second_pass_card_revisions(CHAPTER4_STAGING_QUESTIONS)

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
