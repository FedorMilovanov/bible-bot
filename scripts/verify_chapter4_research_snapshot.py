#!/usr/bin/env python3
"""Verify the vendored Chapter 4 Research handoff against an exact recomputation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENDORED = ROOT / "data" / "chapter4-research-handoff-v2.json"
RESEARCH_SHA = "7e0140129a4aba59a09737701967c3820ff1af57"
AUTHORITY_DIGEST = "1f444991ecc2f180abdbe0f459148ba8dbf0a5045b1d8888e462683c78366c7d"


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--generated-dir", type=Path, required=True)
    args = parser.parse_args()

    generated = args.generated_dir
    summary = load(generated / "integrity-summary.json")
    assert summary["schema_version"] == 2
    assert summary["authority_digest_sha256"] == AUTHORITY_DIGEST
    assert summary["chapter4"] == 72
    assert summary["current_holds"] == 0
    assert summary["competitive_candidates"] == 0

    if not VENDORED.exists():
        print("vendored Chapter 4 v2 snapshot not present yet; bootstrap recomputation passed")
        return

    snapshot = load(VENDORED)
    assert snapshot["schema_version"] == 2
    assert snapshot["research_repository"] == "FedorMilovanov/Research"
    assert snapshot["research_authority_sha"] == RESEARCH_SHA
    assert snapshot["research_authority_digest_sha256"] == AUTHORITY_DIGEST

    generated_ch4 = load(generated / "chapter4-product-handoff.json")["records"]
    assert snapshot["research_records"] == generated_ch4

    generated_proto = load(generated / "prototype-audit.json")["records"]
    ch4_ids = {row["candidate_id"] for row in generated_ch4}
    generated_ch4_proto = [row for row in generated_proto if row.get("candidate_id") in ch4_ids]
    assert snapshot["prototype_records"] == generated_ch4_proto

    generated_rank = load(generated / "ranking-audit.json")["records"]
    generated_ch4_rank = [row for row in generated_rank if row["candidate_id"] in ch4_ids]
    assert snapshot["ranking_records"] == generated_ch4_rank

    print("chapter4 vendored Research handoff v2 matches exact recomputation")


if __name__ == "__main__":
    main()
