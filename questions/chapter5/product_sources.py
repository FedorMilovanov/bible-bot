"""Canonical Chapter 5 source identities for root product admission."""

from .sources import SOURCE_CATALOG as LANE_SOURCE_CATALOG


def _build_identity_catalog() -> dict[str, dict]:
    result = {}
    for source_id, metadata in LANE_SOURCE_CATALOG.items():
        title = str(metadata.get("title") or source_id)
        url = str(metadata.get("url") or "")
        kind = str(metadata.get("kind") or "chapter5_source_identity")
        result[source_id] = {
            "title": title,
            "url": url,
            "kind": kind,
            "product_evidence_status": "identity_only_lane_scoped",
            "source_identity_only": True,
            "chapter5_lanes": ["5:1-14"],
            "known_titles": [title],
            "known_urls": [url],
            "known_kinds": [kind],
            "claim_limit": "Canonical identity only. Claim inspection depth remains in the Chapter-5 lane and is never globally upgraded.",
        }
    return result


SOURCE_CATALOG = _build_identity_catalog()

__all__ = ["SOURCE_CATALOG"]
