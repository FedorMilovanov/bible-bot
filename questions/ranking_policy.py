"""Explicit ranking eligibility policy and review registration."""
from __future__ import annotations

from . import intro_review_extra
from .content_truth import SOURCE_CATALOG
from .content_truth_review import REVIEW_OVERRIDES
from .explanation_depth_review import DEPTH_OVERRIDES
from .geography_review import GEOGRAPHY_OVERRIDES
from .history_witness_review import (
    HISTORY_WITNESS_OVERRIDES,
    HISTORY_WITNESS_SOURCE_CATALOG,
)
from .intro_reception_review import RECEPTION_REVIEW_OVERRIDES
from .legacy_source_review import LEGACY_REVIEW_OVERRIDES, LEGACY_SOURCE_CATALOG
from .nero_review import ADDITIONAL_SOURCE_CATALOG, NERO_OVERRIDES
from .nero_review_extra import EXTRA_SOURCE_CATALOG, NERO_EXTRA_OVERRIDES

# Review layers are merged *per field*, not per card. A later layer may add a
# source, repair a claim class or override the wording, but a partial entry must
# never silently drop fields an earlier layer already reviewed (a plain
# ``dict.update`` did exactly that: a two-key evidence top-up erased the hedged
# wording of ``intro2_04`` and three other introduction cards).
REVIEW_LAYERS = (
    GEOGRAPHY_OVERRIDES,
    DEPTH_OVERRIDES,
    intro_review_extra.INTRO_EXTRA_OVERRIDES,
    RECEPTION_REVIEW_OVERRIDES,
    NERO_OVERRIDES,
    NERO_EXTRA_OVERRIDES,
    # Applied last: the evidence/boundary review supersedes earlier wording-only fixes.
    LEGACY_REVIEW_OVERRIDES,
    HISTORY_WITNESS_OVERRIDES,
)


def _merge_review_layer(target: dict, layer: dict) -> None:
    for question_id, fields in layer.items():
        target.setdefault(question_id, {}).update(fields)


for _layer in REVIEW_LAYERS:
    _merge_review_layer(REVIEW_OVERRIDES, _layer)
SOURCE_CATALOG.update(ADDITIONAL_SOURCE_CATALOG)
SOURCE_CATALOG.update(EXTRA_SOURCE_CATALOG)
SOURCE_CATALOG.update(LEGACY_SOURCE_CATALOG)
SOURCE_CATALOG.update(HISTORY_WITNESS_SOURCE_CATALOG)

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
