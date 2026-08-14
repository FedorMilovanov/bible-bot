#!/usr/bin/env python3
"""Diagnose Chapter 4/5 product mappings against final Research authority.

This diagnostic intentionally reads the Chapter-5 staging bank without importing
its fail-closed reviewed boundary. That allows release CI to report every stale
mapping even when the first stale source would otherwise abort module import.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from questions.chapter4.final_review_registry import PRODUCT_REVIEW_BY_CARD_ID as CH4_REVIEWS
from questions.chapter4.reviewed import CHAPTER4_REVIEWED_QUESTIONS
from questions.chapter5.bank import CHAPTER5_STAGING_QUESTIONS
from questions.research_handoff_v2 import (
    CHAPTER4_RESEARCH_HANDOFF_V2,
    CHAPTER5_RESEARCH_HANDOFF_V2,
)

_CONF = {"contested": 0, "medium": 1, "high": 2}
_WORD = re.compile(r"[A-Za-zА-Яа-яЁё0-9ἀ-῾]+", re.UNICODE)


def _tokens(value: object) -> set[str]:
    return {
        match.group(0).casefold()
        for match in _WORD.finditer(str(value))
        if len(match.group(0)) > 2
    }


def _surface(card: dict) -> str:
    correct = int(card["correct"])
    return " ".join(
        (str(card["question"]), str(card["options"][correct]), str(card["explanation"]))
    )


def _candidate_surface(row: dict) -> str:
    return " ".join(
        (str(row.get("short_claim", "")), str(row.get("product_safe_phrasing", "")))
    )


def _similarity(a: str, b: str) -> float:
    left, right = _tokens(a), _tokens(b)
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _canonical_rows(expanded_dir: Path, chapter: int) -> dict[str, dict]:
    payload = json.loads(
        (expanded_dir / f"chapter{chapter}-product-handoff.json").read_text(
            encoding="utf-8"
        )
    )
    return {str(row["candidate_id"]): row for row in payload["records"]}


def _rank(card: dict, rows: dict[str, dict]) -> list[dict]:
    surface = _surface(card)
    ranked = sorted(
        (
            (_similarity(surface, _candidate_surface(row)), candidate_id)
            for candidate_id, row in rows.items()
        ),
        reverse=True,
    )[:8]
    return [
        {"candidate_id": candidate_id, "score": round(score, 4)}
        for score, candidate_id in ranked
    ]


def _metadata_reasons(card: dict, research: dict) -> list[str]:
    reasons: list[str] = []
    product_position = str(card.get("position"))
    if product_position != research["position"]:
        reasons.append("POSITION_DIFFERS_FROM_RESEARCH")
    product_conf = str(card.get("confidence"))
    research_conf = str(research["confidence"])
    if product_conf not in _CONF or research_conf not in _CONF:
        reasons.append("UNKNOWN_CONFIDENCE_VALUE")
    elif _CONF[product_conf] > _CONF[research_conf]:
        reasons.append("CONFIDENCE_STRONGER_THAN_RESEARCH")
    if str(card.get("claim_type")) != research["claim_type"]:
        reasons.append("CLAIM_TYPE_DIFFERS_FROM_RESEARCH")
    if product_position == "project" and not str(card.get("question", "")).startswith(
        "[Позиция курса]"
    ):
        reasons.append("PROJECT_LABEL_MISSING")
    return reasons


def _audit_ch4(card: dict, rows: dict[str, dict]) -> dict | None:
    review = dict(CH4_REVIEWS[str(card["id"])])
    claim_id = str(review["research_claim_id"])
    research = CHAPTER4_RESEARCH_HANDOFF_V2.get(claim_id)
    reasons: list[str] = []
    if research is None:
        reasons.append("MISSING_RESEARCH_CLAIM")
    else:
        reasons.extend(_metadata_reasons(card, research))
        if review.get("research_effective_claim_digest") != research["effective_claim_digest"]:
            reasons.append("CLAIM_DIGEST_DRIFT")
        review_sources = tuple(review.get("source_ids", ()))
        if review_sources != tuple(research["source_ids"]):
            reasons.append("REVIEW_SOURCE_SET_DIFFERS_FROM_RESEARCH")
        if tuple(review.get("claim_inspection_edge_ids", ())) != tuple(
            research["claim_inspection_edge_ids"]
        ):
            reasons.append("EDGE_ID_DRIFT")
    if not reasons:
        return None
    return {
        "chapter": 4,
        "product_card_id": str(card["id"]),
        "mapped_claim_id": claim_id,
        "reasons": sorted(set(reasons)),
        "top_semantic_candidates": _rank(card, rows),
    }


def _audit_ch5(card: dict, rows: dict[str, dict]) -> dict | None:
    claim_id = str(card.get("research_candidate_id") or "")
    research = CHAPTER5_RESEARCH_HANDOFF_V2.get(claim_id)
    reasons: list[str] = []
    if research is None:
        reasons.append("MISSING_RESEARCH_CLAIM")
    else:
        reasons.extend(_metadata_reasons(card, research))
        product_sources = tuple(str(source_id) for source_id in card.get("sources", ()))
        missing_sources = sorted(set(product_sources) - set(research["source_ids"]))
        if missing_sources:
            reasons.append("SOURCE_NOT_IN_FINAL_RESEARCH_CLAIM")
    if not reasons:
        return None
    return {
        "chapter": 5,
        "product_card_id": str(card["id"]),
        "mapped_claim_id": claim_id,
        "reasons": sorted(set(reasons)),
        "product_sources": list(card.get("sources", ())),
        "research_sources": list(research["source_ids"]) if research else [],
        "top_semantic_candidates": _rank(card, rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expanded-research", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--fail-on-findings", action="store_true")
    args = parser.parse_args()

    expanded = args.expanded_research.resolve()
    ch4_rows = _canonical_rows(expanded, 4)
    ch5_rows = _canonical_rows(expanded, 5)
    findings = [
        finding
        for card in CHAPTER4_REVIEWED_QUESTIONS
        if (finding := _audit_ch4(card, ch4_rows)) is not None
    ]
    findings.extend(
        finding
        for card in CHAPTER5_STAGING_QUESTIONS
        if (finding := _audit_ch5(card, ch5_rows)) is not None
    )

    summary = {
        "chapter4_reviewed": len(CHAPTER4_REVIEWED_QUESTIONS),
        "chapter5_staging": len(CHAPTER5_STAGING_QUESTIONS),
        "finding_count": len(findings),
        "findings": findings,
    }
    args.report.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {key: value for key, value in summary.items() if key != "findings"},
            sort_keys=True,
        )
    )
    if args.fail_on_findings and findings:
        raise SystemExit(f"product/Research semantic audit has {len(findings)} findings")


if __name__ == "__main__":
    main()
