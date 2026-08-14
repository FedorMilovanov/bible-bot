#!/usr/bin/env python3
"""Validate Chapter 4 product v2 against Research prototype and review contracts."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from questions.chapter4.authoring import CHAPTER4_STAGING_QUESTIONS  # noqa: E402
from questions.chapter4.prototype_crosswalk import CHAPTER4_PROTOTYPE_CROSSWALK  # noqa: E402
from questions.chapter4.review_contract import (  # noqa: E402
    validate_all_research_dispositions,
    validate_registry,
)


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", str(value).strip().casefold())


def _load_effective_prototypes(research_root: Path) -> dict[str, dict]:
    data = research_root / "data"
    result: dict[str, dict] = {}
    for path in sorted(data.glob("mcq-prototypes-*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for row in payload.get("prototypes", []):
            result[row["prototype_id"]] = dict(row)
    overrides = json.loads(
        (data / "mcq-prototype-overrides-wave3j2.json").read_text(encoding="utf-8")
    )
    for override in overrides["overrides"]:
        prototype_id = override["prototype_id"]
        if prototype_id in result:
            result[prototype_id].update(
                {
                    "correct": override["correct"],
                    "options": list(override["options"]),
                    "explanation": override["explanation"],
                }
            )
    return result


def _validate_agent_e_snapshot(generated_dir: Path) -> None:
    rows = json.loads(
        (generated_dir / "prototype-audit.json").read_text(encoding="utf-8")
    )["records"]
    by_id = {row["prototype_id"]: row for row in rows}
    assert set(CHAPTER4_PROTOTYPE_CROSSWALK) <= set(by_id)
    for prototype_id, crosswalk in CHAPTER4_PROTOTYPE_CROSSWALK.items():
        audit = by_id[prototype_id]
        assert audit["candidate_id"] == crosswalk["research_claim_id"]
        assert audit["classification"] == crosswalk["agent_e_classification"]
        assert tuple(audit["reasons"]) == crosswalk["agent_e_reasons"]
    counts = Counter(
        row["agent_e_classification"] for row in CHAPTER4_PROTOTYPE_CROSSWALK.values()
    )
    assert counts == Counter(
        {
            "SAFE_TEMPLATE": 13,
            "NEEDS_REWRITE": 10,
            "NONCOMPETITIVE_ONLY": 6,
            "REJECT_AS_PRODUCT_TEMPLATE": 3,
        }
    )


def _validate_no_unsafe_mechanical_copy(research_root: Path) -> None:
    prototypes = _load_effective_prototypes(research_root)
    cards = {card["id"]: card for card in CHAPTER4_STAGING_QUESTIONS}
    for prototype_id, crosswalk in CHAPTER4_PROTOTYPE_CROSSWALK.items():
        if not crosswalk["mechanical_copy_forbidden"]:
            continue
        prototype = prototypes[prototype_id]
        card = cards[crosswalk["product_card_id"]]
        if _norm(card["question"]) == _norm(prototype["question"]):
            raise AssertionError(f"unsafe prototype stem copied mechanically: {prototype_id}")
        correct = int(prototype["correct"])
        wrong_options = {
            _norm(option)
            for index, option in enumerate(prototype["options"])
            if index != correct
        }
        product_options = {_norm(option) for option in card["options"]}
        overlap = wrong_options.intersection(product_options)
        if overlap:
            raise AssertionError(
                f"unsafe prototype distractor copied mechanically: {prototype_id}: {sorted(overlap)}"
            )
        if crosswalk["agent_e_classification"] == "REJECT_AS_PRODUCT_TEMPLATE":
            if not crosswalk["product_resolution"].startswith(
                "INDEPENDENT_PRODUCT_REWRITE"
            ):
                raise AssertionError(
                    f"rejected prototype silently rehabilitated: {prototype_id}"
                )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--research-root", type=Path, required=True)
    parser.add_argument("--generated-dir", type=Path, required=True)
    args = parser.parse_args()

    validate_registry()
    validate_all_research_dispositions()
    _validate_agent_e_snapshot(args.generated_dir)
    _validate_no_unsafe_mechanical_copy(args.research_root)
    print("chapter4-product-v2-contract-ok")


if __name__ == "__main__":
    main()
