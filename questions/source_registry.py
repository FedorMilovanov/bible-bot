"""Canonical source registry assembled from the legacy and chapter-specific catalogs."""

from __future__ import annotations

from .chapter2.sources import SOURCE_CATALOG as CHAPTER2_SOURCE_CATALOG
from .content_truth import SOURCE_CATALOG as LEGACY_SOURCE_CATALOG


def _merge_catalogs(*catalogs: dict[str, dict]) -> dict[str, dict]:
    merged: dict[str, dict] = {}
    for catalog in catalogs:
        for source_id, metadata in catalog.items():
            if source_id in merged and merged[source_id] != metadata:
                raise ValueError(f"Conflicting source metadata for {source_id!r}")
            merged[source_id] = dict(metadata)
    return merged


SOURCE_CATALOG = _merge_catalogs(
    LEGACY_SOURCE_CATALOG,
    CHAPTER2_SOURCE_CATALOG,
)


__all__ = ["SOURCE_CATALOG"]
