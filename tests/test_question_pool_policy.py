import random

from questions import (
    BATTLE_POOL,
    CHALLENGE_POOLS,
    COMPETITIVE_POOL,
    get_pool_by_key,
    pick_competitive_challenge_questions,
)


EXCLUDED_PREFIXES = ("prac", "ling", "intro", "nero", "geo")


def _ids(items):
    return [str(item.get("id") or "") for item in items]


def test_competitive_pool_is_core_only_and_unique():
    ids = _ids(COMPETITIVE_POOL)
    assert len(ids) >= 20
    assert len(ids) == len(set(ids))
    assert all(ids)
    assert not any(qid.startswith(EXCLUDED_PREFIXES) for qid in ids)


def test_battle_pool_uses_exact_competitive_contract():
    assert BATTLE_POOL is COMPETITIVE_POOL


def test_competitive_registry_key_is_not_casual_random_all():
    assert get_pool_by_key("competitive_all") is COMPETITIVE_POOL
    casual_ids = set(_ids(get_pool_by_key("random_all")))
    competitive_ids = set(_ids(COMPETITIVE_POOL))
    assert competitive_ids <= casual_ids
    assert casual_ids - competitive_ids


def test_challenge_pools_exclude_unreviewed_categories():
    assert set(CHALLENGE_POOLS) == {"easy", "medium", "hard"}
    for pool in CHALLENGE_POOLS.values():
        assert not any(qid.startswith(EXCLUDED_PREFIXES) for qid in _ids(pool))


def test_random20_selection_is_exact_unique_and_reproducible():
    first = pick_competitive_challenge_questions("random20", rng=random.Random(7))
    second = pick_competitive_challenge_questions("random20", rng=random.Random(7))
    assert _ids(first) == _ids(second)
    assert len(first) == 20
    assert len(set(_ids(first))) == 20
    assert not any(qid.startswith(EXCLUDED_PREFIXES)) for qid in _ids(first))


def test_hardcore_selection_is_exact_unique_and_core_only():
    selected = pick_competitive_challenge_questions("hardcore20", rng=random.Random(11))
    assert len(selected) == 20
    assert len(set(_ids(selected))) == 20
    assert not any(qid.startswith(EXCLUDED_PREFIXES)) for qid in _ids(selected))
