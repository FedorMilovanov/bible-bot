"""Canonical source registry assembled from legacy and chapter-specific catalogs."""

from __future__ import annotations

from .chapter2.sources import SOURCE_CATALOG as BASE
from .chapter2.sources_11_25 import SOURCE_CATALOG as LATE
from .chapter2.sources_disputed import SOURCE_CATALOG as DISPUTED
from .chapter2.sources_logikon import SOURCE_CATALOG as LOGIKON
from .chapter2.sources_temple import SOURCE_CATALOG as TEMPLE
from .chapter2.sources_visitation import SOURCE_CATALOG as VISITATION
from .content_truth import SOURCE_CATALOG as LEGACY


def _merge_catalogs(*catalogs: dict[str, dict]) -> dict[str, dict]:
    merged: dict[str, dict] = {}
    for catalog in catalogs:
        for source_id, metadata in catalog.items():
            if source_id in merged and merged[source_id] != metadata:
                raise ValueError(f"Conflicting source metadata for {source_id!r}")
            merged[source_id] = dict(metadata)
    return merged


SOURCE_CATALOG = _merge_catalogs(LEGACY, BASE, LATE, DISPUTED, LOGIKON, TEMPLE, VISITATION)

__all__ = ["SOURCE_CATALOG"]
