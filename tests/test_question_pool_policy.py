import random

from questions import (
    BATTLE_POOL,
    CHAPTER1_COMPETITIVE_POOL,
    CHAPTER3_AUTHORIZED_COMPETITIVE_POOL,
    COMPETITIVE_POOL,
    NON_COMPETITIVE_IDS,
    get_pool_by_key,
    pick_competitive_challenge_questions,
)


EXCLUDED_COMPETITIVE_PREFIXES = ("prac", "ling", "intro", "nero", "geo")
SOURCE_REVIEWED_COMPETITIVE_IDS = {"easy_12", "med_02", "hard_02", "hard_12"}


def _ids(items):
    return [str(item.get("id") or "") for item in items]


def test_general_competitive_is_explicit_while_legacy_random_remains_chapter1_context():
    assert BATTLE_POOL is COMPETITIVE_POOL
    assert get_pool_by_key("competitive_all") is COMPETITIVE_POOL

    casual_ids = set(_ids(get_pool_by_key("random_all")))
    chapter1_competitive_ids = set(_ids(CHAPTER1_COMPETITIVE_POOL))
    chapter3_authorized_ids = set(_ids(CHAPTER3_AUTHORIZED_COMPETITIVE_POOL))
    competitive_ids = set(_ids(COMPETITIVE_POOL))

    assert chapter1_competitive_ids < casual_ids
    assert chapter3_authorized_ids
    assert chapter3_authorized_ids.isdisjoint(casual_ids)
    assert competitive_ids == chapter1_competitive_ids | chapter3_authorized_ids
    assert chapter1_competitive_ids.isdisjoint(chapter3_authorized_ids)


def test_competitive_pool_is_unique_and_quarantines_debated_items():
    ids = _ids(COMPETITIVE_POOL)
    assert len(ids) >= 20
    assert len(ids) == len(set(ids))
    assert all(ids)
    assert not (set(ids) & NON_COMPETITIVE_IDS)
    assert not any(qid.startswith(EXCLUDED_COMPETITIVE_PREFIXES) for qid in ids)


def test_source_reviewed_items_are_competitive():
    competitive_ids = set(_ids(COMPETITIVE_POOL))
    assert SOURCE_REVIEWED_COMPETITIVE_IDS <= competitive_ids
    assert not (SOURCE_REVIEWED_COMPETITIVE_IDS & NON_COMPETITIVE_IDS)


def test_casual_random_keeps_noncompetitive_learning_categories():
    casual_ids = _ids(get_pool_by_key("random_all"))
    assert any(qid.startswith("prac") for qid in casual_ids)
    assert any(qid.startswith("ling") for qid in casual_ids)
    assert any(qid.startswith("intro") for qid in casual_ids)
    assert any(qid.startswith("nero") for qid in casual_ids)
    assert any(qid.startswith("geo") for qid in casual_ids)
    assert not any(qid.startswith("ch3_") for qid in casual_ids)


def test_twenty_question_modes_are_unique_ranking_eligible_and_challenge_taxonomized():
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
        assert not any(qid.startswith("ch3_") for qid in ids)
