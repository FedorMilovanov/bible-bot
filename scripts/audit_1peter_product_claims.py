#!/usr/bin/env python3
"""Audit Chapter 4/5 product cards against the final canonical Research handoff."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from questions.chapter4.final_review_registry import PRODUCT_REVIEW_BY_CARD_ID as CH4_REVIEWS
from questions.chapter4.reviewed import CHAPTER4_REVIEWED_QUESTIONS
from questions.chapter5.review_contract_v2 import PRODUCT_REVIEW_RECORDS as CH5_REVIEWS
from questions.chapter5.reviewed import CHAPTER5_REVIEWED_QUESTIONS
from questions.research_handoff_v2 import (
    CHAPTER4_RESEARCH_HANDOFF_V2,
    CHAPTER5_RESEARCH_HANDOFF_V2,
)

_CONF = {"contested": 0, "medium": 1, "high": 2}
_WORD = re.compile(r"[A-Za-zА-Яа-яЁё0-9ἀ-῾]+", re.UNICODE)


def _tokens(value: object) -> set[str]:
    return {match.group(0).casefold() for match in _WORD.finditer(str(value)) if len(match.group(0)) > 2}


def _surface(card: dict) -> str:
    correct = int(card["correct"])
    return " ".join((str(card["question"]), str(card["options"][correct]), str(card["explanation"])))


def _candidate_surface(row: dict) -> str:
    return " ".join((str(row.get("short_claim", "")), str(row.get("product_safe_phrasing", ""))))


def _similarity(a: str, b: str) -> float:
    left, right = _tokens(a), _tokens(b)
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _canonical_rows(expanded_dir: Path, chapter: int) -> dict[str, dict]:
    payload = json.loads((expanded_dir / f"chapter{chapter}-product-handoff.json").read_text(encoding="utf-8"))
    return {str(row["candidate_id"]): row for row in payload["records"]}


def _prototype_map(expanded_dir: Path) -> dict[str, dict]:
    payload = json.loads((expanded_dir / "prototype-audit.json").read_text(encoding="utf-8"))
    return {str(row["prototype_id"]): row for row in payload["records"]}


def _audit_card(
    card: dict,
    review: dict,
    canonical: dict[str, dict],
    canonical_rows: dict[str, dict],
    prototypes: dict[str, dict],
    *,
    chapter: int,
) -> dict | None:
    claim_id = str(review.get("research_claim_id") or review.get("effective_research_claim", {}).get("candidate_id") or card.get("research_candidate_id") or "")
    research = canonical.get(claim_id)
    reasons: list[str] = []
    if research is None:
        reasons.append("MISSING_RESEARCH_CLAIM")
    else:
        review_digest = review.get("research_effective_claim_digest") or review.get("claim_digest")
        if review_digest != research["effective_claim_digest"]:
            reasons.append("CLAIM_DIGEST_DRIFT")
        review_edges = tuple(review.get("claim_inspection_edge_ids", ()))
        product_sources = tuple(review.get("source_ids") or review.get("source_subset") or card.get("sources") or ())
        expected_edge_by_source = dict(zip(research["source_ids"], research["claim_inspection_edge_ids"], strict=True))
        if any(source not in expected_edge_by_source for source in product_sources):
            reasons.append("SOURCE_NOT_IN_FINAL_RESEARCH_CLAIM")
        else:
            expected_edges = tuple(expected_edge_by_source[source] for source in product_sources)
            if review_edges != expected_edges:
                reasons.append("EDGE_ID_DRIFT")
        product_position = str(card.get("position"))
        if product_position != research["position"]:
            # A project/application card may narrow a neutral claim only through an
            # explicit independent review; flag it for human release readback.
            reasons.append("POSITION_DIFFERS_FROM_RESEARCH")
        product_conf = str(card.get("confidence"))
        if product_conf not in _CONF or _CONF[product_conf] > _CONF[str(research["confidence"])]:
            reasons.append("CONFIDENCE_STRONGER_THAN_RESEARCH")
        product_type = str(card.get("claim_type"))
        if product_type != research["claim_type"]:
            reasons.append("CLAIM_TYPE_DIFFERS_FROM_RESEARCH")
        if product_position == "project" and not str(card.get("question", "")).startswith("[Позиция курса]"):
            reasons.append("PROJECT_LABEL_MISSING")

        prototype_review = review.get("prototype_review") or {}
        prototype_id = prototype_review.get("research_prototype_id") or review.get("prototype_id")
        review_class = prototype_review.get("research_prototype_classification") or review.get("prototype_classification")
        if prototype_id:
            canonical_proto = prototypes.get(str(prototype_id))
            if canonical_proto is None:
                reasons.append("UNKNOWN_RESEARCH_PROTOTYPE")
            elif review_class != canonical_proto.get("classification"):
                reasons.append("PROTOTYPE_DISPOSITION_DRIFT")
            if canonical_proto and canonical_proto.get("classification") in {"REJECT_AS_PRODUCT_TEMPLATE", "NEEDS_REWRITE"}:
                decision = str(
                    prototype_review.get("prototype_usage_decision")
                    or review.get("product_rewrite_disposition")
                    or ""
                )
                if "INDEPENDENT_PRODUCT_REWRITE" not in decision:
                    reasons.append("UNSAFE_PROTOTYPE_WITHOUT_INDEPENDENT_REWRITE")

    if not reasons:
        return None
    card_surface = _surface(card)
    ranked = sorted(
        (
            (_similarity(card_surface, _candidate_surface(row)), candidate_id)
            for candidate_id, row in canonical_rows.items()
        ),
        reverse=True,
    )[:5]
    return {
        "chapter": chapter,
        "product_card_id": str(card.get("id")),
        "mapped_claim_id": claim_id,
        "reasons": sorted(set(reasons)),
        "top_semantic_candidates": [
            {"candidate_id": candidate_id, "score": round(score, 4)}
            for score, candidate_id in ranked
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expanded-research", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--fail-on-findings", action="store_true")
    args = parser.parse_args()

    expanded = args.expanded_research.resolve()
    prototypes = _prototype_map(expanded)
    ch4_rows = _canonical_rows(expanded, 4)
    ch5_rows = _canonical_rows(expanded, 5)
    findings: list[dict] = []

    for card in CHAPTER4_REVIEWED_QUESTIONS:
        review = dict(CH4_REVIEWS[str(card["id"])])
        finding = _audit_card(
            card, review, CHAPTER4_RESEARCH_HANDOFF_V2, ch4_rows, prototypes, chapter=4
        )
        if finding:
            findings.append(finding)
    for card in CHAPTER5_REVIEWED_QUESTIONS:
        review = CH5_REVIEWS[str(card["id"])]
        finding = _audit_card(
            card, review, CHAPTER5_RESEARCH_HANDOFF_V2, ch5_rows, prototypes, chapter=5
        )
        if finding:
            findings.append(finding)

    summary = {
        "chapter4_reviewed": len(CHAPTER4_REVIEWED_QUESTIONS),
        "chapter5_reviewed": len(CHAPTER5_REVIEWED_QUESTIONS),
        "finding_count": len(findings),
        "findings": findings,
    }
    args.report.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k != "findings"}, sort_keys=True))
    if args.fail_on_findings and findings:
        raise SystemExit(f"product/Research semantic audit has {len(findings)} findings")


if __name__ == "__main__":
    main()
