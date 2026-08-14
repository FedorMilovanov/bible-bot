import questions
from questions.chapter2.reviewed import CHAPTER2_REVIEWED_QUESTIONS


def _ids(items):
    return {str(item["id"]) for item in items}


def test_chapter2_registry_is_exactly_the_reviewed_bank():
    expected = _ids(CHAPTER2_REVIEWED_QUESTIONS)
    actual = _ids(questions.get_pool_by_key("chapter2"))
    assert actual == expected
    assert len(actual) == len(CHAPTER2_REVIEWED_QUESTIONS)
    assert len(actual) >= 10


def test_chapter2_is_not_admitted_to_ranked_or_battle_pools():
    chapter2_ids = _ids(questions.get_pool_by_key("chapter2"))
    assert chapter2_ids.isdisjoint(_ids(questions.COMPETITIVE_POOL))
    assert chapter2_ids.isdisjoint(_ids(questions.BATTLE_POOL))


def test_chapter2_is_not_admitted_to_challenge_pools_or_legacy_random_all():
    chapter2_ids = _ids(questions.get_pool_by_key("chapter2"))
    assert chapter2_ids.isdisjoint(_ids(questions.get_pool_by_key("random_all")))
    for pool in questions.CHALLENGE_POOLS.values():
        assert chapter2_ids.isdisjoint(_ids(pool))
