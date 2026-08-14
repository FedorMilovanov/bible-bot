#!/usr/bin/env python3
"""Diagnose Chapter 4/5 product mappings against final Research authority.

This tool intentionally avoids importing ``questions``. The root package is a
production fail-closed boundary and must remain free to reject stale Chapter-5
metadata. Release diagnostics read authoring data and the immutable vendored
handoff directly so one bad card cannot hide the rest of the mismatch set.
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import runpy
from pathlib import Path

_CONF = {"contested": 0, "medium": 1, "high": 2}
_WORD = re.compile(r"\w+", re.UNICODE)
ROOT = Path(__file__).resolve().parents[1]


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


def _card_snapshot(card: dict) -> dict:
    correct = int(card["correct"])
    return {
        "verse": card.get("verse"),
        "topic": card.get("topic"),
        "question": card.get("question"),
        "keyed_answer": card["options"][correct],
        "explanation": card.get("explanation"),
        "claim_type": card.get("claim_type"),
        "confidence": card.get("confidence"),
        "position": card.get("position"),
        "sources": list(card.get("sources", ())),
    }


def _candidate_snapshot(candidate_id: str, row: dict) -> dict:
    return {
        "candidate_id": candidate_id,
        "reference": row.get("reference"),
        "short_claim": row.get("short_claim"),
        "product_safe_phrasing": row.get("product_safe_phrasing"),
        "claim_type": row.get("claim_type"),
        "confidence": row.get("confidence"),
        "position": row.get("position"),
        "source_ids": list(row.get("source_ids", ())),
        "status": row.get("status"),
    }


def _canonical_rows(expanded_dir: Path, chapter: int) -> dict[str, dict]:
    payload = json.loads(
        (expanded_dir / f"chapter{chapter}-product-handoff.json").read_text(
            encoding="utf-8"
        )
    )
    return {str(row["candidate_id"]): row for row in payload["records"]}


def _vendored_claims() -> tuple[dict, dict[str, dict]]:
    payload = json.loads(
        (ROOT / "data" / "1peter-research-handoff-v2.json").read_text(encoding="utf-8")
    )
    return payload, {str(row["candidate_id"]): row for row in payload["claims"]}


def _load_data_file(path: Path, variable: str) -> list[dict]:
    namespace = runpy.run_path(str(path))
    return list(namespace[variable])


def _chapter4_rows() -> dict[str, tuple]:
    source = (ROOT / "questions" / "chapter4" / "review_registry.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "_ROWS" for target in node.targets
        ):
            rows = ast.literal_eval(node.value)
            return {str(row[0]): tuple(row) for row in rows}
    raise RuntimeError("cannot locate Chapter-4 _ROWS literal")


def _rank(card: dict, rows: dict[str, dict]) -> list[dict]:
    surface = _surface(card)
    ranked = sorted(
        (
            (_similarity(surface, _candidate_surface(row)), candidate_id)
            for candidate_id, row in rows.items()
        ),
        reverse=True,
    )[:8]
    result: list[dict] = []
    for score, candidate_id in ranked:
        item = _candidate_snapshot(candidate_id, rows[candidate_id])
        item["score"] = round(score, 4)
        result.append(item)
    return result


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
    return reasons


def _audit_ch4(
    card: dict,
    mapping: tuple,
    vendored: dict[str, dict],
    rows: dict[str, dict],
) -> dict | None:
    # _ROWS tuple schema: card_id, review_record_id, content_digest,
    # research_claim_id, claimed_position, claimed_confidence, claimed_type,
    # prototype_id, prototype_classification.
    claim_id = str(mapping[3])
    research = vendored.get(claim_id)
    reasons: list[str] = []
    if research is None or research["chapter"] != 4:
        reasons.append("MISSING_RESEARCH_CLAIM")
    else:
        reasons.extend(_metadata_reasons(card, research))
        declared = (str(mapping[4]), str(mapping[5]), str(mapping[6]))
        runtime = (
            str(card.get("position")),
            str(card.get("confidence")),
            str(card.get("claim_type")),
        )
        if declared != runtime:
            reasons.append("PRODUCT_REVIEW_DECLARATION_DIFFERS_FROM_CARD")
        prototype_id = str(mapping[7] or "")
        prototype_class = str(mapping[8] or "")
        if prototype_id:
            matches = [
                p for p in research.get("prototypes", ())
                if str(p.get("prototype_id")) == prototype_id
            ]
            if len(matches) != 1:
                reasons.append("PROTOTYPE_NOT_OWNED_BY_FINAL_CLAIM")
            elif prototype_class != str(matches[0].get("classification")):
                reasons.append("PROTOTYPE_DISPOSITION_DRIFT")
    if not reasons:
        return None
    current_row = rows.get(claim_id)
    return {
        "chapter": 4,
        "product_card_id": str(card["id"]),
        "mapped_claim_id": claim_id,
        "reasons": sorted(set(reasons)),
        "product_card": _card_snapshot(card),
        "mapped_research_claim": (
            _candidate_snapshot(claim_id, current_row) if current_row else None
        ),
        "top_semantic_candidates": _rank(card, rows),
    }


def _audit_ch5(
    card: dict,
    vendored: dict[str, dict],
    rows: dict[str, dict],
) -> dict | None:
    claim_id = str(card.get("research_candidate_id") or "")
    research = vendored.get(claim_id)
    reasons: list[str] = []
    if research is None or research["chapter"] != 5:
        reasons.append("MISSING_RESEARCH_CLAIM")
    else:
        reasons.extend(_metadata_reasons(card, research))
        product_sources = tuple(str(source_id) for source_id in card.get("sources", ()))
        missing_sources = sorted(set(product_sources) - set(research["source_ids"]))
        if missing_sources:
            reasons.append("SOURCE_NOT_IN_FINAL_RESEARCH_CLAIM")
    if not reasons:
        return None
    current_row = rows.get(claim_id)
    return {
        "chapter": 5,
        "product_card_id": str(card["id"]),
        "mapped_claim_id": claim_id,
        "reasons": sorted(set(reasons)),
        "product_card": _card_snapshot(card),
        "mapped_research_claim": (
            _candidate_snapshot(claim_id, current_row) if current_row else None
        ),
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
    header, vendored = _vendored_claims()
    if header.get("research_authority_sha") != "0142430af8ba80f28e0fd9cde669d32611a1d2af":
        raise SystemExit("vendored Research authority SHA drift")
    if header.get("authority_digest_sha256") != "1f444991ecc2f180abdbe0f459148ba8dbf0a5045b1d8888e462683c78366c7d":
        raise SystemExit("vendored Research authority digest drift")

    ch4_cards = _load_data_file(
        ROOT / "questions" / "chapter4" / "authoring.py", "CHAPTER4_STAGING_QUESTIONS"
    )
    ch5_cards = _load_data_file(
        ROOT / "questions" / "chapter5" / "bank.py", "CHAPTER5_STAGING_QUESTIONS"
    )
    ch4_mapping = _chapter4_rows()

    findings: list[dict] = []
    for card in ch4_cards:
        mapping = ch4_mapping.get(str(card["id"]))
        if mapping is None:
            findings.append(
                {
                    "chapter": 4,
                    "product_card_id": str(card["id"]),
                    "mapped_claim_id": "",
                    "reasons": ["MISSING_PRODUCT_REVIEW_MAPPING"],
                    "product_card": _card_snapshot(card),
                    "mapped_research_claim": None,
                    "top_semantic_candidates": _rank(card, ch4_rows),
                }
            )
            continue
        finding = _audit_ch4(card, mapping, vendored, ch4_rows)
        if finding:
            findings.append(finding)
    for card in ch5_cards:
        finding = _audit_ch5(card, vendored, ch5_rows)
        if finding:
            findings.append(finding)

    summary = {
        "chapter4_staging": len(ch4_cards),
        "chapter5_staging": len(ch5_cards),
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
