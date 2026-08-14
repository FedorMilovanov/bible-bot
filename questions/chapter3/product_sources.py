"""Canonical Chapter 3 source identities for root product admission.

Claim evidence remains lane-local. This module intentionally collapses only work
identity (title/url/kind/lane presence) and never chooses the strongest inspection
status from multiple lanes.
"""

from __future__ import annotations

from .sources import SOURCE_CATALOG as LANE_18_22
from .sources_1_7 import SOURCE_CATALOG as LANE_1_7
from .sources_8_12 import SOURCE_CATALOG as LANE_8_12
from .sources_13_17 import SOURCE_CATALOG as LANE_13_17

LANE_SOURCE_CATALOGS = {
    "3:1-7": LANE_1_7,
    "3:8-12": LANE_8_12,
    "3:13-17": LANE_13_17,
    "3:18-22": LANE_18_22,
}


def _first_text(records: list[dict], key: str, fallback: str = "") -> str:
    for record in records:
        value = str(record.get(key) or "").strip()
        if value:
            return value
    return fallback


def _build_identity_catalog() -> dict[str, dict]:
    by_id: dict[str, list[tuple[str, dict]]] = {}
    for lane, catalog in LANE_SOURCE_CATALOGS.items():
        for source_id, metadata in catalog.items():
            by_id.setdefault(source_id, []).append((lane, metadata))

    result: dict[str, dict] = {}
    for source_id, lane_records in by_id.items():
        lanes = [lane for lane, _ in lane_records]
        records = [metadata for _, metadata in lane_records]
        urls = sorted({str(record.get("url") or "").strip() for record in records if record.get("url")})
        titles = sorted({str(record.get("title") or "").strip() for record in records if record.get("title")})
        kinds = sorted({str(record.get("kind") or "").strip() for record in records if record.get("kind")})

        result[source_id] = {
            "title": _first_text(records, "title", source_id),
            "url": _first_text(records, "url"),
            "kind": _first_text(records, "kind", "chapter3_source_identity"),
            "product_evidence_status": "identity_only_lane_scoped",
            "source_identity_only": True,
            "chapter3_lanes": sorted(lanes),
            "known_titles": titles,
            "known_urls": urls,
            "known_kinds": kinds,
            "claim_limit": (
                "Canonical product identity only. Claim-level inspection depth, "
                "rights state, and passage support must be read from the reviewed "
                "card's own Chapter-3 lane catalog; this record never upgrades them."
            ),
        }
    return result


SOURCE_CATALOG = _build_identity_catalog()

__all__ = ["LANE_SOURCE_CATALOGS", "SOURCE_CATALOG"]
