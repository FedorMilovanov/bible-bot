"""Canonical Chapter 4 Research handoff v2 projection.

The authority identity and claim/source edge identities come from the vendored
immutable Research release metadata. No Research network access occurs at runtime,
and source inspection depth is intentionally not copied into the root registry.
"""
from __future__ import annotations

from ..research_handoff_v2 import CHAPTER4_RESEARCH_HANDOFF_V2
from ..research_release_authority import (
    RESEARCH_AUTHORITY_DIGEST_SHA256,
    RESEARCH_AUTHORITY_SHA,
    RESEARCH_HANDOFF_SCHEMA_VERSION,
    RESEARCH_REPOSITORY,
)

RESEARCH_HANDOFF_V2 = {
    candidate_id: {
        "research_claim_id": candidate_id,
        "research_effective_claim_digest": record["effective_claim_digest"],
        "position": record["position"],
        "confidence": record["confidence"],
        "claim_type": record["claim_type"],
        "source_ids": tuple(record["source_ids"]),
        "claim_inspection_edge_ids": tuple(record["claim_inspection_edge_ids"]),
        "effective_status": record["effective_status"],
        "ranking_discrepancy_candidate": candidate_id == "w3q_123",
    }
    for candidate_id, record in CHAPTER4_RESEARCH_HANDOFF_V2.items()
}

if len(RESEARCH_HANDOFF_V2) != 72:
    raise ValueError("Chapter 4 Research v2 projection must contain exactly 72 claims")
if any(
    len(record["source_ids"]) != len(record["claim_inspection_edge_ids"])
    for record in RESEARCH_HANDOFF_V2.values()
):
    raise ValueError("every Chapter 4 source identity must retain its exact canonical edge")

__all__ = [
    "RESEARCH_REPOSITORY",
    "RESEARCH_AUTHORITY_SHA",
    "RESEARCH_AUTHORITY_DIGEST_SHA256",
    "RESEARCH_HANDOFF_SCHEMA_VERSION",
    "RESEARCH_HANDOFF_V2",
]
