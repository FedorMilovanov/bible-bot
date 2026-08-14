import json
from collections import Counter
from pathlib import Path

import questions
from questions.chapter3.challenge_taxonomy import (
    CHAPTER3_CHALLENGE_RATIONALE,
    CHAPTER3_CHALLENGE_TAXONOMY,
    taxonomy_ids,
)
from questions.chapter3.ranking_authority import CHAPTER3_RANKING_AUTHORIZED_IDS
from questions.chapter3.reviewed import CHAPTER3_REVIEWED_QUESTIONS

MANIFEST_PATH = Path(__file__).resolve().parents[1] / "data" / "chapter3-challenge-taxonomy.json"
MANIFEST = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _ids(items):
    return {str(item["id"]) for item in items}


def test_taxonomy_covers_explicit_authority_exactly_once():
    flattened = [qid for ids in CHAPTER3_CHALLENGE_TAXONOMY.values() for qid in ids]
    assert len(flattened) == len(set(flattened)) == 12
    assert taxonomy_ids() == CHAPTER3_RANKING_AUTHORIZED_IDS
    assert set(CHAPTER3_CHALLENGE_RATIONALE) == set(CHAPTER3_RANKING_AUTHORIZED_IDS)


def test_taxonomy_is_six_easy_six_medium_and_no_forced_hard_cards():
    assert len(CHAPTER3_CHALLENGE_TAXONOMY["easy"]) == 6
    assert len(CHAPTER3_CHALLENGE_TAXONOMY["medium"]) == 6
    assert CHAPTER3_CHALLENGE_TAXONOMY["hard"] == ()
    assert MANIFEST["counts"] == {"easy": 6, "medium": 6, "hard": 0}
    assert MANIFEST["hard_empty_by_design"] is True
    assert "direct-text" in MANIFEST["hard_empty_reason"]


def test_every_taxonomy_card_is_reviewed_authorized_text_high_neutral():
    reviewed = {item["id"]: item for item in CHAPTER3_REVIEWED_QUESTIONS}
    for qid in taxonomy_ids():
        item = reviewed[qid]
        assert item["claim_type"] == "text"
        assert item["confidence"] == "high"
        assert item["position"] == "neutral"
        assert item["competitive"] is True
        assert set(item["sources"]) == {"sblgnt", "net_1p3_1_7"}
        assert CHAPTER3_CHALLENGE_RATIONALE[qid].strip()


def test_nonempty_taxonomy_buckets_do_not_create_answer_position_leakage():
    reviewed = {item["id"]: item for item in CHAPTER3_REVIEWED_QUESTIONS}
    for level in ("easy", "medium"):
        positions = Counter(reviewed[qid]["correct"] for qid in CHAPTER3_CHALLENGE_TAXONOMY[level])
        assert set(positions) == {0, 1, 2, 3}
        assert max(positions.values()) - min(positions.values()) <= 1, (level, positions)


def test_taxonomy_checkpoint_still_does_not_modify_challenge_pools():
    chapter3_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    for level, pool in questions.CHALLENGE_POOLS.items():
        assert chapter3_ids.isdisjoint(_ids(pool)), level
    assert chapter3_ids.isdisjoint(_ids(questions.CHALLENGE_FALLBACK_POOL))
    assert MANIFEST["challenge_pools_mutated"] is False
    assert MANIFEST["challenge_fallback_mutated"] is False
    assert MANIFEST["automatic_admission"] is False


def test_manifest_taxonomy_matches_python_authority():
    assert MANIFEST["status"] == "CHALLENGE_TAXONOMY_REVIEWED_NOT_ADMITTED"
    assert MANIFEST["parent_battle_head"] == "6f7fbc5de2e7fbfc7d54b6c844234636dbc03a49"
    assert MANIFEST["authorized_count"] == 12
    assert MANIFEST["taxonomy_covers_authority_exactly"] is True
    for level in ("easy", "medium", "hard"):
        assert tuple(MANIFEST["taxonomy"][level]) == CHAPTER3_CHALLENGE_TAXONOMY[level]
