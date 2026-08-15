#!/usr/bin/env python3
"""Fail-closed release validator for the canonical 1 Peter Chapter 1-5 stack."""
# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import questions
from course_catalog import SURFACE_MINIAPP, SURFACE_TELEGRAM, list_courses
from questions.chapter4.research_handoff import RESEARCH_HANDOFF_V2 as CH4_RESEARCH
from questions.chapter4.review_registry import PRODUCT_REVIEW_BY_CARD_ID as CH4_REVIEWS
from questions.chapter4.reviewed import CHAPTER4_REVIEWED_QUESTIONS
from questions.chapter5.review_contract_v2 import (
    PRODUCT_REVIEW_RECORDS as CH5_REVIEWS,
    validate_full_bank as validate_ch5_bank,
)
from questions.chapter5.reviewed import CHAPTER5_REVIEWED_QUESTIONS
from questions.pool_policy import get_pool_policy
from questions.research_handoff_v2 import CHAPTER5_RESEARCH_HANDOFF_V2
from questions.research_release_authority import (
    RESEARCH_AUTHORITY_DIGEST_SHA256,
    RESEARCH_AUTHORITY_SHA,
    RESEARCH_HANDOFF_SCHEMA_VERSION,
    RESEARCH_RELEASE_REPOSITORY_SHA,
)
from questions.source_registry import SOURCE_CATALOG

EXPECTED_RESEARCH_RELEASE = "8d6e5bc3f303d0a6a2d1a15969e042907f3387db"
EXPECTED_RESEARCH_AUTHORITY = "0142430af8ba80f28e0fd9cde669d32611a1d2af"
EXPECTED_RESEARCH_DIGEST = "1f444991ecc2f180abdbe0f459148ba8dbf0a5045b1d8888e462683c78366c7d"
CONFIDENCE = {"contested": 0, "medium": 1, "high": 2}


def _ids(items) -> set[str]:
    return {str(item.get("id") or "").strip() for item in items}


def _assert_learning_policy(pool_key: str) -> None:
    policy = get_pool_policy(pool_key)
    assert policy.scoring_mode == "learning", pool_key
    assert policy.ranked is False, pool_key
    assert policy.points_per_question == 0, pool_key


def _assert_ch4_reviews() -> None:
    cards = {str(card["id"]): card for card in CHAPTER4_REVIEWED_QUESTIONS}
    assert set(cards) == set(CH4_REVIEWS)
    assert len(cards) == 52
    for card_id, card in cards.items():
        review = CH4_REVIEWS[card_id]
        claim_id = str(review["research_claim_id"])
        research = CH4_RESEARCH[claim_id]
        assert review["research_effective_claim_digest"] == research["research_effective_claim_digest"]
        assert tuple(review["source_ids"]) == tuple(research["source_ids"])
        assert tuple(review["claim_inspection_edge_ids"]) == tuple(research["claim_inspection_edge_ids"])
        assert review["claimed_position"] == research["position"] == card["position"]
        assert review["claimed_claim_type"] == research["claim_type"] == card["claim_type"]
        assert CONFIDENCE[str(card["confidence"])] <= CONFIDENCE[str(research["confidence"])]
        assert card.get("competitive") is False
        if card["position"] == "project":
            assert str(card["question"]).startswith("[Позиция курса]")


def _assert_ch5_reviews() -> None:
    validate_ch5_bank()
    cards = {str(card["id"]): card for card in CHAPTER5_REVIEWED_QUESTIONS}
    assert set(cards) == set(CH5_REVIEWS)
    assert len(cards) == 72
    for card_id, card in cards.items():
        review = CH5_REVIEWS[card_id]
        claim_id = str(card["research_candidate_id"])
        research = CHAPTER5_RESEARCH_HANDOFF_V2[claim_id]
        assert review["claim_digest"] == research["effective_claim_digest"]
        expected_edges = tuple(
            research["claim_inspection_edge_ids"][tuple(research["source_ids"]).index(source_id)]
            for source_id in card["sources"]
        )
        assert tuple(review["claim_inspection_edge_ids"]) == expected_edges
        assert set(card["sources"]).issubset(set(research["source_ids"]))
        assert card["position"] == research["position"]
        assert card["claim_type"] == research["claim_type"]
        assert CONFIDENCE[str(card["confidence"])] <= CONFIDENCE[str(research["confidence"])]
        assert card.get("competitive") is False
        if card["position"] == "project":
            assert str(card["question"]).startswith("[Позиция курса]")


def validate() -> dict:
    assert RESEARCH_RELEASE_REPOSITORY_SHA == EXPECTED_RESEARCH_RELEASE
    assert RESEARCH_AUTHORITY_SHA == EXPECTED_RESEARCH_AUTHORITY
    assert RESEARCH_AUTHORITY_DIGEST_SHA256 == EXPECTED_RESEARCH_DIGEST
    assert RESEARCH_HANDOFF_SCHEMA_VERSION == 2

    counts = {
        "chapter1": len(questions.all_chapter1_questions),
        "chapter2": len(questions.chapter2_questions),
        "chapter3_learning": len(questions.chapter3_questions),
        "chapter3_competitive": len(questions.CHAPTER3_AUTHORIZED_COMPETITIVE_POOL),
        "chapter4_reviewed": len(questions.chapter4_questions),
        "chapter5_reviewed": len(questions.chapter5_questions),
    }
    assert counts["chapter3_learning"] == 165
    assert counts["chapter3_competitive"] == 12
    assert counts["chapter4_reviewed"] == 52
    assert counts["chapter5_reviewed"] == 72

    for key in ("chapter2", "chapter3", "chapter4", "chapter5"):
        _assert_learning_policy(key)

    learning_ids = {
        key: _ids(questions.POOL_REGISTRY[key])
        for key in ("chapter2", "chapter3", "chapter4", "chapter5")
    }
    random_ids = _ids(questions.POOL_REGISTRY["random_all"])
    assert all(random_ids.isdisjoint(ids) for ids in learning_ids.values())
    ch45 = learning_ids["chapter4"] | learning_ids["chapter5"]
    assert _ids(questions.BATTLE_POOL).isdisjoint(ch45)
    assert _ids(questions.COMPETITIVE_POOL).isdisjoint(ch45)
    assert _ids(questions.CHALLENGE_FALLBACK_POOL).isdisjoint(ch45)
    for pool in questions.CHALLENGE_POOLS.values():
        assert _ids(pool).isdisjoint(ch45)

    chapter3_competitive_ids = _ids(questions.CHAPTER3_AUTHORIZED_COMPETITIVE_POOL)
    assert len(chapter3_competitive_ids) == 12
    assert chapter3_competitive_ids <= learning_ids["chapter3"]

    tg = {entry.key for entry in list_courses(surface=SURFACE_TELEGRAM)}
    mini = {entry.key for entry in list_courses(surface=SURFACE_MINIAPP)}
    for key in ("chapter2", "chapter3", "chapter4", "chapter5"):
        assert key in tg and key in mini

    forbidden_depth = {
        "inspection_scope",
        "evidence_status",
        "claim_inspection_edge_ids",
        "strongest_depth",
        "claim_depth",
    }
    for source_id, metadata in SOURCE_CATALOG.items():
        if metadata.get("source_identity_only") is True:
            leaked = forbidden_depth.intersection(metadata)
            assert not leaked, (source_id, sorted(leaked))

    _assert_ch4_reviews()
    _assert_ch5_reviews()

    return {
        "status": "PASS",
        "research_release_repository_sha": RESEARCH_RELEASE_REPOSITORY_SHA,
        "research_authority_sha": RESEARCH_AUTHORITY_SHA,
        "research_authority_digest_sha256": RESEARCH_AUTHORITY_DIGEST_SHA256,
        "counts": counts,
        "chapter4_competitive": 0,
        "chapter5_competitive": 0,
        "catalog_policy_equal": True,
        "identity_registry_depth_upgrade": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = validate()
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.write_text(text, encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
