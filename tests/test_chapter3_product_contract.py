import questions
from questions.chapter3.reviewed import CHAPTER3_REVIEWED_QUESTIONS


def _ids(items):
    return {str(item["id"]) for item in items}


def test_chapter3_registry_is_exactly_the_reviewed_bank():
    expected = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    actual_pool = questions.get_pool_by_key("chapter3")
    actual = _ids(actual_pool)

    assert actual == expected
    assert len(actual_pool) == len(CHAPTER3_REVIEWED_QUESTIONS) == 165
    assert actual_pool == questions.chapter3_questions


def test_chapter3_normal_learning_does_not_expand_legacy_random_all():
    chapter3_ids = _ids(questions.get_pool_by_key("chapter3"))
    random_ids = _ids(questions.get_pool_by_key("random_all"))
    assert chapter3_ids.isdisjoint(random_ids)


def test_chapter3_is_not_admitted_to_ranked_battle_or_challenge_pools():
    chapter3_ids = _ids(questions.get_pool_by_key("chapter3"))

    assert chapter3_ids.isdisjoint(_ids(questions.COMPETITIVE_POOL))
    assert chapter3_ids.isdisjoint(_ids(questions.BATTLE_POOL))
    for key, pool in questions.CHALLENGE_POOLS.items():
        assert chapter3_ids.isdisjoint(_ids(pool)), key


def test_internal_competitive_metadata_does_not_change_root_ranking_membership():
    metadata_candidates = {
        item["id"]
        for item in CHAPTER3_REVIEWED_QUESTIONS
        if item["competitive"] is True
    }
    assert metadata_candidates
    assert metadata_candidates.isdisjoint(_ids(questions.COMPETITIVE_POOL))
    assert metadata_candidates.isdisjoint(_ids(questions.BATTLE_POOL))
