import json
from pathlib import Path
import random

import questions
from questions.chapter3.challenge_taxonomy import CHAPTER3_CHALLENGE_TAXONOMY
from questions.chapter3.ranking_authority import CHAPTER3_RANKING_AUTHORIZED_IDS
from questions.chapter3.reviewed import CHAPTER3_REVIEWED_QUESTIONS
from questions.pool_policy import is_non_scoring_learning_pool

MANIFEST_PATH = Path(__file__).resolve().parents[1] / "data" / "chapter3-challenge-admission.json"
MANIFEST = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _ids(items):
    return {str(item["id"]) for item in items}


def test_chapter3_challenge_pools_match_reviewed_taxonomy_exactly():
    reviewed_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    authorized = set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    unauthorized = reviewed_ids - authorized

    for level in ("easy", "medium", "hard"):
        expected = set(CHAPTER3_CHALLENGE_TAXONOMY[level])
        actual_local = _ids(questions.CHAPTER3_CHALLENGE_POOLS[level])
        actual_global = reviewed_ids & _ids(questions.CHALLENGE_POOLS[level])
        assert actual_local == expected, level
        assert actual_global == expected, level

    assert _ids(questions.CHAPTER3_CHALLENGE_POOLS["hard"]) == set()
    assert unauthorized.isdisjoint(
        set().union(*(_ids(pool) for pool in questions.CHALLENGE_POOLS.values()))
    )
    assert len(unauthorized) == 153


def test_challenge_fallback_remains_chapter1_only_and_disjoint_from_chapter3():
    reviewed_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    assert questions.CHALLENGE_FALLBACK_POOL is questions.CHAPTER1_COMPETITIVE_POOL
    assert reviewed_ids.isdisjoint(_ids(questions.CHALLENGE_FALLBACK_POOL))


def test_challenge_category_id_sets_are_disjoint():
    category_ids = {level: _ids(pool) for level, pool in questions.CHALLENGE_POOLS.items()}
    assert category_ids["easy"].isdisjoint(category_ids["medium"])
    assert category_ids["easy"].isdisjoint(category_ids["hard"])
    assert category_ids["medium"].isdisjoint(category_ids["hard"])


def test_seeded_challenge_selection_preserves_quotas_and_never_leaks_unauthorized_chapter3():
    reviewed_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    authorized = set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    unauthorized = reviewed_ids - authorized
    category_ids = {level: _ids(pool) for level, pool in questions.CHALLENGE_POOLS.items()}
    distributions = {
        "random20": {"easy": 6, "medium": 6, "hard": 8},
        "hardcore20": {"easy": 4, "medium": 4, "hard": 12},
    }

    selected_chapter3_somewhere = False
    for mode, expected_counts in distributions.items():
        for seed in range(256):
            selected = questions.pick_competitive_challenge_questions(
                mode,
                rng=random.Random(seed),
            )
            selected_ids = [item["id"] for item in selected]
            selected_set = set(selected_ids)

            assert len(selected_ids) == 20
            assert len(selected_set) == 20
            assert selected_set.isdisjoint(unauthorized), (mode, seed)
            assert (selected_set & reviewed_ids) <= authorized, (mode, seed)
            assert (selected_set & reviewed_ids).isdisjoint(category_ids["hard"]), (mode, seed)

            for level, expected in expected_counts.items():
                assert len(selected_set & category_ids[level]) == expected, (mode, seed, level)

            if selected_set & authorized:
                selected_chapter3_somewhere = True

    assert selected_chapter3_somewhere is True


def test_normal_learning_and_legacy_random_remain_separate_after_challenge_admission():
    reviewed_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    assert is_non_scoring_learning_pool("chapter3") is True
    assert _ids(questions.get_pool_by_key("chapter3")) == reviewed_ids
    assert reviewed_ids.isdisjoint(_ids(questions.get_pool_by_key("random_all")))


def test_manifest_matches_live_challenge_admission():
    assert MANIFEST["status"] == "CHALLENGE_ADMITTED_BY_REVIEWED_TAXONOMY"
    assert MANIFEST["parent_taxonomy_head"] == "62fb982da58a4a97739dd9b504e43cb5d34d7ec2"
    assert MANIFEST["authorized_count"] == 12
    assert MANIFEST["challenge_counts"] == {"easy": 6, "medium": 6, "hard": 0}
    assert MANIFEST["challenge_fallback_source"] == "CHAPTER1_COMPETITIVE_POOL"
    assert MANIFEST["fallback_chapter3_allowed"] is False
    assert MANIFEST["hard_chapter3_allowed"] is False
    assert MANIFEST["unauthorized_chapter3_count"] == 153
    assert MANIFEST["unauthorized_chapter3_challenge"] == 0
    assert MANIFEST["normal_learning_non_scoring"] is True
    assert MANIFEST["battle_authorized_count"] == 12
    assert MANIFEST["random_all_admission"] is False

    for level in ("easy", "medium", "hard"):
        assert tuple(MANIFEST["challenge_taxonomy"][level]) == CHAPTER3_CHALLENGE_TAXONOMY[level]
