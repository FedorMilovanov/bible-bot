import questions
from questions.chapter3.challenge_taxonomy import CHAPTER3_CHALLENGE_TAXONOMY
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
    assert chapter3_ids & _ids(questions.COMPETITIVE_POOL) == authorized
    assert chapter3_ids & _ids(questions.BATTLE_POOL) == authorized
    assert unauthorized.isdisjoint(_ids(questions.COMPETITIVE_POOL))
    assert unauthorized.isdisjoint(_ids(questions.BATTLE_POOL))
    assert len(unauthorized) == 153


def test_challenge_consumes_exact_reviewed_taxonomy_and_no_unauthorized_chapter3():
    chapter3_ids = _ids(questions.get_pool_by_key("chapter3"))
    authorized = set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    unauthorized = chapter3_ids - authorized

    for level in ("easy", "medium", "hard"):
        expected = set(CHAPTER3_CHALLENGE_TAXONOMY[level])
        assert chapter3_ids & _ids(questions.CHALLENGE_POOLS[level]) == expected
        assert _ids(questions.CHAPTER3_CHALLENGE_POOLS[level]) == expected

    challenge_ch3 = chapter3_ids & set().union(
        *(_ids(pool) for pool in questions.CHALLENGE_POOLS.values())
    )
    assert challenge_ch3 == authorized
    assert unauthorized.isdisjoint(challenge_ch3)
    assert chapter3_ids.isdisjoint(_ids(questions.CHALLENGE_FALLBACK_POOL))
    assert not CHAPTER3_CHALLENGE_TAXONOMY["hard"]


def test_internal_competitive_metadata_does_not_expand_beyond_authority():
    metadata_candidates = {
        item["id"]
        for item in CHAPTER3_REVIEWED_QUESTIONS
        if item["competitive"] is True
    }
    assert metadata_candidates == set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    assert metadata_candidates == (_ids(questions.COMPETITIVE_POOL) & _ids(CHAPTER3_REVIEWED_QUESTIONS))
    assert metadata_candidates == (_ids(questions.BATTLE_POOL) & _ids(CHAPTER3_REVIEWED_QUESTIONS))
