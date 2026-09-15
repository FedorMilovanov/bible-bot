"""Canonical source registry assembled from legacy and chapter-specific catalogs."""

from __future__ import annotations

from .chapter2.sources import SOURCE_CATALOG as BASE
from .chapter2.sources_11_25 import SOURCE_CATALOG as LATE
from .chapter2.sources_disputed import SOURCE_CATALOG as DISPUTED
from .chapter2.sources_logikon import SOURCE_CATALOG as LOGIKON
from .chapter2.sources_temple import SOURCE_CATALOG as TEMPLE
from .chapter2.sources_visitation import SOURCE_CATALOG as VISITATION
from .chapter3.product_sources import SOURCE_CATALOG as CHAPTER3_PRODUCT_IDENTITIES
from .chapter4.product_sources import SOURCE_CATALOG as CHAPTER4_PRODUCT_IDENTITIES
from .chapter5.product_sources import SOURCE_CATALOG as CHAPTER5_PRODUCT_IDENTITIES
from .content_truth import SOURCE_CATALOG as LEGACY


def _merge_catalogs(*catalogs: dict[str, dict]) -> dict[str, dict]:
    merged: dict[str, dict] = {}
    for catalog in catalogs:
        for source_id, metadata in catalog.items():
            if source_id in merged and merged[source_id] != metadata:
                raise ValueError(f"Conflicting source metadata for {source_id!r}")
            merged[source_id] = dict(metadata)
    return merged


def _extend_with_identity_only_sources(
    base: dict[str, dict],
    identity_catalog: dict[str, dict],
) -> dict[str, dict]:
    """Add identities while preserving claim depth from the earliest authority.

    A later identity-only catalog may know a stable bibliographic URL that an
    earlier identity-only Research handoff intentionally left blank. Filling that
    URL is identity enrichment, not an evidence-depth upgrade. All authority,
    inspection and claim-depth fields still come from the earlier record.
    """

    merged = {source_id: dict(metadata) for source_id, metadata in base.items()}
    for source_id, metadata in identity_catalog.items():
        if source_id in merged:
            current = merged[source_id]
            if (
                current.get("source_identity_only") is True
                and metadata.get("source_identity_only") is True
            ):
                current_url = str(current.get("url") or "").strip()
                later_url = str(metadata.get("url") or "").strip()
                if current_url and later_url and current_url != later_url:
                    raise ValueError(
                        f"Conflicting identity URLs for {source_id!r}: "
                        f"{current_url!r} != {later_url!r}"
                    )
                if not current_url and later_url:
                    current["url"] = later_url
            # Earlier canonical authority still wins. A later chapter may reuse
            # the same work ID, but cannot upgrade or replace shared claim depth.
            continue
        merged[source_id] = dict(metadata)
    return merged


_BASE_SOURCE_CATALOG = _merge_catalogs(
    LEGACY,
    BASE,
    LATE,
    DISPUTED,
    LOGIKON,
    TEMPLE,
    VISITATION,
)
SOURCE_CATALOG = _extend_with_identity_only_sources(
    _BASE_SOURCE_CATALOG,
    CHAPTER3_PRODUCT_IDENTITIES,
)
SOURCE_CATALOG = _extend_with_identity_only_sources(
    SOURCE_CATALOG,
    CHAPTER4_PRODUCT_IDENTITIES,
)
SOURCE_CATALOG = _extend_with_identity_only_sources(
    SOURCE_CATALOG,
    CHAPTER5_PRODUCT_IDENTITIES,
)

__all__ = ["SOURCE_CATALOG"]
