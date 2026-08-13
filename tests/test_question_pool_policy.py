import random

from questions import (
    BATTLE_POOL,
    COMPETITIVE_POOL,
    NON_COMPETITIVE_IDS,
    get_pool_by_key,
    pick_competitive_challenge_questions,
)


EXCLUDED_COMPETITIVE_PREFIXES = ("prac", "ling", "intro", "nero", "geo")


def _ids(items):
    return [str(item.get("id") or "") for item in items]


def test_competitive_pool_is_explicit_and_narrower_than_casual_random():
    assert BATTLE_POOL is COMPETITIVE_POOL
    assert get_pool_by_key("competitive_all") is COMPETITIVE_POOL

    casual_ids = set(_ids(get_pool_by_key("random_all")))
    competitive_ids = set(_ids(COMPETITIVE_POOL))
    assert competitive_ids < casual_ids


def test_competitive_pool_is_unique_and_quarantines_debated_items():
    ids = _ids(COMPETITIVE_POOL)
    assert len(ids) >= 20
    assert len(ids) == len(set(ids))
    assert all(ids)
    assert not (set(ids) & NON_COMPETITIVE_IDS)
    assert not any(qid.startswith(EXCLUDED_COMPETITIVE_PREFIXES) for qid in ids)


def test_casual_random_keeps_noncompetitive_learning_categories():
    casual_ids = _ids(get_pool_by_key("random_all"))
    assert any(qid.startswith("prac") for qid in casual_ids)
    assert any(qid.startswith("ling") for qid in casual_ids)
    assert any(qid.startswith("intro") for qid in casual_ids)
    assert any(qid.startswith("nero") for qid in casual_ids)
    assert any(qid.startswith("geo") for qid in casual_ids)


def test_twenty_question_modes_are_unique_and_ranking_eligible():
    for mode in ("random20", "hardcore20"):
        selected = pick_competitive_challenge_questions(
            mode,
            rng=random.Random(7),
        )
        ids = _ids(selected)
        assert len(ids) == 20
        assert len(set(ids)) == 20
        assert not (set(ids) & NON_COMPETITIVE_IDS)
        assert not any(qid.startswith(EXCLUDED_COMPETITIVE_PREFIXES) for qid in ids)
