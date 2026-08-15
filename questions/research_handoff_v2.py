"""Canonical local projection of the immutable Research handoff v2.

The compressed JSON payload is vendored release metadata only. It contains claim
identities, claim/source edge identities and prototype dispositions, but no
inspection-depth upgrade for the runtime source registry and no cross-repository
network access.
"""
from __future__ import annotations

import json
import lzma
from pathlib import Path
from types import MappingProxyType

from .research_release_authority import (
    RESEARCH_AUTHORITY_DIGEST_SHA256,
    RESEARCH_AUTHORITY_SHA,
    RESEARCH_HANDOFF_SCHEMA_VERSION,
    RESEARCH_RELEASE_REPOSITORY_SHA,
)

_MANIFEST_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "1peter-research-handoff-v2.json.xz"
)
_payload = json.loads(lzma.decompress(_MANIFEST_PATH.read_bytes()).decode("utf-8"))

if _payload.get("schema_version") != RESEARCH_HANDOFF_SCHEMA_VERSION:
    raise ValueError("vendored Research handoff schema drift")
if _payload.get("research_release_repository_sha") != RESEARCH_RELEASE_REPOSITORY_SHA:
    raise ValueError("vendored Research release repository SHA drift")
if _payload.get("research_authority_sha") != RESEARCH_AUTHORITY_SHA:
    raise ValueError("vendored Research authority SHA drift")
if _payload.get("authority_digest_sha256") != RESEARCH_AUTHORITY_DIGEST_SHA256:
    raise ValueError("vendored Research authority digest drift")

_records: dict[str, MappingProxyType] = {}
for raw in _payload.get("claims", ()):
    candidate_id = str(raw["candidate_id"])
    if candidate_id in _records:
        raise ValueError(f"duplicate vendored Research claim: {candidate_id}")
    record = dict(raw)
    record["source_ids"] = tuple(record.get("source_ids", ()))
    record["claim_inspection_edge_ids"] = tuple(record.get("claim_inspection_edge_ids", ()))
    record["prototypes"] = tuple(
        MappingProxyType(dict(item)) for item in record.get("prototypes", ())
    )
    if len(record["effective_claim_digest"]) != 64:
        raise ValueError(f"invalid effective claim digest: {candidate_id}")
    if len(record["source_ids"]) != len(record["claim_inspection_edge_ids"]):
        raise ValueError(f"source/edge cardinality mismatch: {candidate_id}")
    _records[candidate_id] = MappingProxyType(record)

RESEARCH_HANDOFF_V2 = MappingProxyType(_records)
CHAPTER4_RESEARCH_HANDOFF_V2 = MappingProxyType(
    {cid: record for cid, record in _records.items() if record["chapter"] == 4}
)
CHAPTER5_RESEARCH_HANDOFF_V2 = MappingProxyType(
    {cid: record for cid, record in _records.items() if record["chapter"] == 5}
)

if len(RESEARCH_HANDOFF_V2) != 144:
    raise ValueError("Research handoff must contain exactly 144 claims")
if len(CHAPTER4_RESEARCH_HANDOFF_V2) != 72 or len(CHAPTER5_RESEARCH_HANDOFF_V2) != 72:
    raise ValueError("Research handoff must contain exactly 72 claims per chapter")
if any(record["competitive_candidate"] for record in RESEARCH_HANDOFF_V2.values()):
    raise ValueError("Research handoff unexpectedly grants Chapter 4/5 competitive authority")

__all__ = [
    "RESEARCH_HANDOFF_V2",
    "CHAPTER4_RESEARCH_HANDOFF_V2",
    "CHAPTER5_RESEARCH_HANDOFF_V2",
]
