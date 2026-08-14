import questions
from questions.chapter3.ranking_authority import CHAPTER3_RANKING_AUTHORIZED_IDS
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


def test_only_explicit_authority_enters_general_competitive_and_battle():
    chapter3_ids = _ids(questions.get_pool_by_key("chapter3"))
    authorized = set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    unauthorized = chapter3_ids - authorized

    competitive_ch3 = chapter3_ids & _ids(questions.COMPETITIVE_POOL)
    battle_ch3 = chapter3_ids & _ids(questions.BATTLE_POOL)

    assert competitive_ch3 == authorized
    assert battle_ch3 == authorized
    assert unauthorized.isdisjoint(_ids(questions.COMPETITIVE_POOL))
    assert unauthorized.isdisjoint(_ids(questions.BATTLE_POOL))
    assert len(unauthorized) == 153


def test_chapter3_stays_out_of_challenge_without_difficulty_taxonomy():
    chapter3_ids = _ids(questions.get_pool_by_key("chapter3"))
    for key, pool in questions.CHALLENGE_POOLS.items():
        assert chapter3_ids.isdisjoint(_ids(pool)), key
    assert chapter3_ids.isdisjoint(_ids(questions.CHALLENGE_FALLBACK_POOL))


def test_internal_competitive_metadata_does_not_expand_beyond_authority():
    metadata_candidates = {
        item["id"]
        for item in CHAPTER3_REVIEWED_QUESTIONS
        if item["competitive"] is True
    }
    assert metadata_candidates == set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    assert metadata_candidates == (_ids(questions.COMPETITIVE_POOL) & _ids(CHAPTER3_REVIEWED_QUESTIONS))
    assert metadata_candidates == (_ids(questions.BATTLE_POOL) & _ids(CHAPTER3_REVIEWED_QUESTIONS))
