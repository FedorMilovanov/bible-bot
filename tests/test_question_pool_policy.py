import random

from questions import (
    BATTLE_POOL,
    COMPETITIVE_POOL,
    NON_COMPETITIVE_IDS,
    get_pool_by_key,
    pick_competitive_challenge_questions,
)


def _ids(items):
    return [str(item.get("id") or "") for item in items]


def test_pool_aliases_are_explicit():
    assert BATTLE_POOL is COMPETITIVE_POOL
    assert get_pool_by_key("competitive_all") is COMPETITIVE_POOL
    assert get_pool_by_key("random_all") is COMPETITIVE_POOL


def test_learning_aggregate_is_broader_than_scored_pool():
    study_ids = set(_ids(get_pool_by_key("learning_all")))
    scored_ids = set(_ids(COMPETITIVE_POOL))
    assert scored_ids < study_ids


def test_excluded_ids_never_enter_scored_pool():
    ids = _ids(COMPETITIVE_POOL)
    assert len(ids) >= 20
    assert len(ids) == len(set(ids))
    assert not (set(ids) & NON_COMPETITIVE_IDS)


def test_twenty_question_modes_are_unique():
    for mode in ("random20", "hardcore20"):
        selected = pick_competitive_challenge_questions(
            mode,
            rng=random.Random(7),
        )
        ids = _ids(selected)
        assert len(ids) == 20
        assert len(set(ids)) == 20
        assert not (set(ids) & NON_COMPETITIVE_IDS)
