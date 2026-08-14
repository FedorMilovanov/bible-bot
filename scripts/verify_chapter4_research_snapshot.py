#!/usr/bin/env python3
"""Verify product-side Chapter 4 v2 projection against exact Research recomputation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from questions.chapter4.research_handoff import (
    RESEARCH_AUTHORITY_DIGEST_SHA256,
    RESEARCH_AUTHORITY_SHA,
    RESEARCH_HANDOFF_SCHEMA_VERSION,
    RESEARCH_HANDOFF_V2,
    RESEARCH_REPOSITORY,
)


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--generated-dir", type=Path, required=True)
    args = parser.parse_args()
    generated = args.generated_dir

    summary = load(generated / "integrity-summary.json")
    assert summary["schema_version"] == RESEARCH_HANDOFF_SCHEMA_VERSION == 2
    assert summary["authority_digest_sha256"] == RESEARCH_AUTHORITY_DIGEST_SHA256
    assert summary["chapter4"] == 72
    assert summary["current_holds"] == 0
    assert summary["competitive_candidates"] == 0
    assert RESEARCH_REPOSITORY == "FedorMilovanov/Research"
    assert RESEARCH_AUTHORITY_SHA == "7e0140129a4aba59a09737701967c3820ff1af57"

    ranking_rows = load(generated / "ranking-audit.json")["records"]
    discrepancy = {
        row["candidate_id"]: bool(row["discrepancy_candidate"])
        for row in ranking_rows
    }

    generated_rows = load(generated / "chapter4-product-handoff.json")["records"]
    assert len(generated_rows) == 72
    generated_projection = {}
    for row in generated_rows:
        claim_id = row["candidate_id"]
        generated_projection[claim_id] = {
            "research_claim_id": claim_id,
            "research_effective_claim_digest": row["effective_claim_digest"],
            "position": row["position"],
            "confidence": row["confidence"],
            "claim_type": row["claim_type"],
            "source_ids": tuple(row["source_ids"]),
            "claim_inspection_edge_ids": tuple(
                edge["claim_inspection_edge_id"] for edge in row["source_evidence"]
            ),
            "effective_status": row["effective_status"],
            "ranking_discrepancy_candidate": discrepancy[claim_id],
        }

    assert generated_projection == dict(RESEARCH_HANDOFF_V2)
    print("chapter4 Research handoff v2 projection matches exact immutable recomputation")


if __name__ == "__main__":
    main()
