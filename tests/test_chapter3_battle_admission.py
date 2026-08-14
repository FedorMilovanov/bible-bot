import json
from pathlib import Path
import random

import questions
from questions.chapter3.challenge_taxonomy import CHAPTER3_CHALLENGE_TAXONOMY
from questions.chapter3.ranking_authority import CHAPTER3_RANKING_AUTHORIZED_IDS
from questions.chapter3.reviewed import CHAPTER3_REVIEWED_QUESTIONS
from questions.pool_policy import is_non_scoring_learning_pool

MANIFEST_PATH = Path(__file__).resolve().parents[1] / "data" / "chapter3-battle-admission.json"
MANIFEST = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _ids(items):
    return {str(item["id"]) for item in items}


def test_general_competitive_and_battle_add_exactly_the_twelve_authorized_ids():
    chapter3_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    authorized = set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    unauthorized = chapter3_ids - authorized
    assert len(chapter3_ids) == 165
    assert len(authorized) == 12
    assert len(unauthorized) == 153
    assert chapter3_ids & _ids(questions.CHAPTER3_AUTHORIZED_COMPETITIVE_POOL) == authorized
    assert chapter3_ids & _ids(questions.COMPETITIVE_POOL) == authorized
    assert chapter3_ids & _ids(questions.BATTLE_POOL) == authorized
    assert unauthorized.isdisjoint(_ids(questions.COMPETITIVE_POOL))
    assert unauthorized.isdisjoint(_ids(questions.BATTLE_POOL))


def test_general_competitive_is_chapter1_authority_plus_exact_chapter3_authority():
    chapter1_ids = _ids(questions.CHAPTER1_COMPETITIVE_POOL)
    chapter3_ids = _ids(questions.CHAPTER3_AUTHORIZED_COMPETITIVE_POOL)
    combined_ids = _ids(questions.COMPETITIVE_POOL)
    assert chapter1_ids.isdisjoint(chapter3_ids)
    assert chapter3_ids == set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    assert combined_ids == chapter1_ids | chapter3_ids
    assert len(questions.COMPETITIVE_POOL) == len(combined_ids)
    assert questions.BATTLE_POOL is questions.COMPETITIVE_POOL
    assert questions.get_pool_by_key("competitive_all") is questions.COMPETITIVE_POOL


def test_later_challenge_layer_consumes_taxonomy_but_keeps_fallback_chapter1_only():
    chapter3_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    authorized = set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    assert questions.CHALLENGE_FALLBACK_POOL is questions.CHAPTER1_COMPETITIVE_POOL
    assert chapter3_ids.isdisjoint(_ids(questions.CHALLENGE_FALLBACK_POOL))
    challenge_ch3 = set()
    for level, pool in questions.CHALLENGE_POOLS.items():
        expected = set(CHAPTER3_CHALLENGE_TAXONOMY[level])
        actual = chapter3_ids & _ids(pool)
        assert actual == expected, level
        challenge_ch3 |= actual
    assert challenge_ch3 == authorized
    assert not CHAPTER3_CHALLENGE_TAXONOMY["hard"]


def test_seeded_challenge_selection_never_leaks_unauthorized_chapter3():
    chapter3_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    authorized = set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    unauthorized = chapter3_ids - authorized
    saw_authorized = False
    for mode in ("random20", "hardcore20"):
        for seed in range(128):
            selected = questions.pick_competitive_challenge_questions(
                mode,
                rng=random.Random(seed),
            )
            selected_ids = {item["id"] for item in selected}
            assert len(selected) == 20
            assert len(selected_ids) == 20
            assert selected_ids.isdisjoint(unauthorized), (mode, seed)
            assert (selected_ids & chapter3_ids) <= authorized, (mode, seed)
            saw_authorized = saw_authorized or bool(selected_ids & authorized)
    assert saw_authorized is True


def test_chapter3_normal_learning_stays_non_scoring_after_ranked_mode_admission():
    assert is_non_scoring_learning_pool("chapter3") is True
    chapter3_ids = _ids(questions.get_pool_by_key("chapter3"))
    assert chapter3_ids == _ids(CHAPTER3_REVIEWED_QUESTIONS)
    assert chapter3_ids.isdisjoint(_ids(questions.get_pool_by_key("random_all")))


def test_battle_manifest_remains_historical_pre_challenge_checkpoint():
    assert MANIFEST["status"] == "BATTLE_ADMITTED_CHALLENGE_CLOSED"
    assert MANIFEST["parent_authority_head"] == "05fbcc7f2052cdd106d406dc364bc466dac843fa"
    assert MANIFEST["authorized_count"] == 12
    assert set(MANIFEST["authorized_ids"]) == set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    assert MANIFEST["general_competitive_admission"] is True
    assert MANIFEST["battle_admission"] is True
    assert MANIFEST["challenge_admission"] is False
    assert MANIFEST["challenge_taxonomy_defined"] is False
    assert MANIFEST["challenge_fallback_source"] == "CHAPTER1_COMPETITIVE_POOL"
    assert MANIFEST["random_all_admission"] is False
    assert MANIFEST["normal_learning_non_scoring"] is True
    assert MANIFEST["unauthorized_chapter3_count"] == 153
    assert MANIFEST["unauthorized_chapter3_ranked"] == 0
    assert MANIFEST["challenge_leakage_allowed"] is False
