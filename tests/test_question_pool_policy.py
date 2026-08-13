import random

from questions import (
    BATTLE_POOL,
    CHALLENGE_POOLS,
    COMPETITIVE_POOL,
    NON_COMPETITIVE_IDS,
    get_pool_by_key,
    pick_competitive_challenge_questions,
)


EXCLUDED_PREFIXES = ("prac", "ling", "intro", "nero", "geo")
KNOWN_DEBATED_CORE_IDS = {
    "easy_02",
    "easy_03",
    "easy_04",
    "easy_12",
    "med_01",
    "med_02",
    "med_03",
    "med_13",
    "med_15",
    "hard_02",
    "hard_03",
    "hard_11",
    "hard_12",
    "hard_13",
}


def _ids(items):
    return [str(item.get("id") or "") for item in items]


def test_competitive_pool_is_unique_and_ranking_eligible():
    ids = _ids(COMPETITIVE_POOL)
    assert len(ids) >= 20
    assert len(ids) == len(set(ids))
    assert all(ids)
    assert not any(qid.startswith(EXCLUDED_PREFIXES) for qid in ids)
    assert not (set(ids) & NON_COMPETITIVE_IDS)


def test_known_debated_core_ids_are_quarantined_from_ranking():
    assert KNOWN_DEBATED_CORE_IDS <= NON_COMPETITIVE_IDS
    assert not (set(_ids(COMPETITIVE_POOL)) & KNOWN_DEBATED_CORE_IDS)


def test_battle_pool_uses_exact_competitive_contract():
    assert BATTLE_POOL is COMPETITIVE_POOL


def test_competitive_registry_key_is_not_casual_random_all():
    assert get_pool_by_key("competitive_all") is COMPETITIVE_POOL
    casual_ids = set(_ids(get_pool_by_key("random_all")))
    competitive_ids = set(_ids(COMPETITIVE_POOL))
    assert competitive_ids <= casual_ids
    assert casual_ids - competitive_ids


def test_challenge_pools_apply_same_quarantine_policy():
    assert set(CHALLENGE_POOLS) == {"easy", "medium", "hard"}
    for pool in CHALLENGE_POOLS.values():
        ids = _ids(pool)
        assert not any(qid.startswith(EXCLUDED_PREFIXES) for qid in ids)
        assert not (set(ids) & NON_COMPETITIVE_IDS)


def test_random20_selection_is_exact_unique_and_reproducible():
    first = pick_competitive_challenge_questions("random20", rng=random.Random(7))
    second = pick_competitive_challenge_questions("random20", rng=random.Random(7))
    assert _ids(first) == _ids(second)
    assert len(first) == 20
    assert len(set(_ids(first))) == 20
    assert not any(qid.startswith(EXCLUDED_PREFIXES) for qid in _ids(first))
    assert not (set(_ids(first)) & NON_COMPETITIVE_IDS)


def test_hardcore_selection_is_exact_unique_and_ranking_eligible():
    selected = pick_competitive_challenge_questions(
        "hardcore20",
        rng=random.Random(11),
    )
    assert len(selected) == 20
    assert len(set(_ids(selected))) == 20
    assert not any(qid.startswith(EXCLUDED_PREFIXES) for qid in _ids(selected))
    assert not (set(_ids(selected)) & NON_COMPETITIVE_IDS)
