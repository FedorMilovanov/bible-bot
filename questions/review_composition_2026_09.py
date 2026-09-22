"""2026-09 review-layer composition for the frozen Chapter 1-5 release bank.

The release-integrity workflow freezes the legacy authority files
(``content_truth.py``, ``content_truth_review.py``, ``ranking_policy.py``,
``chapter1.py``, ``intro.py``) at the Chapter 1-5 release base. The 2026
quality pass cannot edit those files in place, so every correction lives in
non-frozen overlay tables and is composed here into the singletons the
canonical chain already consumes:

* ``content_truth.QUESTION_OVERRIDES`` — curate-stage repairs, equivalent to an
  edit in the authoring corpus, applied before any later review layer;
* ``content_truth_review.REVIEW_OVERRIDES`` — review-stage repairs, applied on
  top of curation;
* ``content_truth.SOURCE_CATALOG`` — additional vetted sources.

Layers merge *per field*, never per card: a later evidence top-up (sources,
claim class) must not erase wording an earlier layer already reviewed. All
layer payloads are deep-copied before merging, so composing the singletons can
never mutate an overlay table through a shared nested reference (the frozen
``ranking_policy`` merge is per card and otherwise aliases nested dicts). The
explicit ``REVIEW_LAYERS`` order is the merge order; the evidence/boundary
layers are last by design.
"""
from __future__ import annotations

import importlib.util
from copy import deepcopy
from pathlib import Path

from . import intro_review_extra
from .authority_corrections_2026_09 import (
    CORRECTION_SOURCE_CATALOG,
    CURATE_STAGE_CORRECTIONS,
    REVIEW_STAGE_CORRECTIONS,
)
from .content_truth import QUESTION_OVERRIDES, SOURCE_CATALOG
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
from .seminary_depth_review_2026_09 import SEMINARY_REVIEW_OVERRIDES, SEMINARY_SOURCE_CATALOG


def _pristine_base_review_overrides() -> dict:
    """Reload the frozen base REVIEW_OVERRIDES untouched by any merge.

    The frozen ``ranking_policy`` performs per-card ``dict.update`` merges at
    import time, which may already have replaced nested entries in the live
    singleton. The base table as authored is re-read from its file so the
    per-field rebuild below starts from exactly the released base.
    """
    base_path = Path(__file__).with_name("content_truth_review.py")
    spec = importlib.util.spec_from_file_location("_pristine_content_truth_review", base_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return deepcopy(module.REVIEW_OVERRIDES)


# Review-stage order. The released content-truth-review table (plus the 2026
# field repairs) is layer zero.
REVIEW_LAYERS = (
    REVIEW_STAGE_CORRECTIONS,
    GEOGRAPHY_OVERRIDES,
    DEPTH_OVERRIDES,
    intro_review_extra.INTRO_EXTRA_OVERRIDES,
    RECEPTION_REVIEW_OVERRIDES,
    SEMINARY_REVIEW_OVERRIDES,
    NERO_OVERRIDES,
    NERO_EXTRA_OVERRIDES,
    # Applied last: the evidence/boundary review supersedes earlier wording-only fixes.
    LEGACY_REVIEW_OVERRIDES,
    HISTORY_WITNESS_OVERRIDES,
)

SOURCE_LAYERS = (
    ADDITIONAL_SOURCE_CATALOG,
    EXTRA_SOURCE_CATALOG,
    CORRECTION_SOURCE_CATALOG,
    SEMINARY_SOURCE_CATALOG,
    LEGACY_SOURCE_CATALOG,
    HISTORY_WITNESS_SOURCE_CATALOG,
)


def _merge_fields(target: dict, layer: dict) -> None:
    for question_id, fields in layer.items():
        target.setdefault(question_id, {}).update(deepcopy(fields))


def compose() -> None:
    """Rebuild the frozen review singletons from every 2026 overlay."""
    for question_id, fields in CURATE_STAGE_CORRECTIONS.items():
        QUESTION_OVERRIDES.setdefault(question_id, {}).update(deepcopy(fields))

    composed = _pristine_base_review_overrides()
    for layer in REVIEW_LAYERS:
        _merge_fields(composed, layer)
    REVIEW_OVERRIDES.clear()
    REVIEW_OVERRIDES.update(composed)

    for layer in SOURCE_LAYERS:
        SOURCE_CATALOG.update(deepcopy(layer))


compose()

__all__ = ["REVIEW_LAYERS", "compose"]
