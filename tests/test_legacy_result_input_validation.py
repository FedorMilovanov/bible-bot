import pytest

import database
import legacy_result_store as store


class ExplodingCollection:
    def __getattr__(self, name):
        raise AssertionError(f"invalid result must fail before Mongo access: {name}")


def _apply(**overrides):
    kwargs = {
        "result_id": "validation-result",
        "user_id": 42,
        "username": "u",
        "first_name": "User",
        "level_key": "easy",
        "score": 1,
        "total": 2,
        "time_seconds": 3.0,
        "score_multiplier": 1.0,
        "max_streak": 1,
        "challenge_mode": None,
        "quiz_mode": "relaxed",
        "fastest_answer": None,
    }
    kwargs.update(overrides)
    return store.apply_base_result_once(**kwargs)


@pytest.fixture(autouse=True)
def no_mongo_access(monkeypatch):
    monkeypatch.setattr(database, "collection", ExplodingCollection())


def test_invalid_level_key_is_not_defaulted_to_easy():
    with pytest.raises(ValueError, match="unsupported level_key"):
        _apply(level_key="not-a-level")


def test_score_above_total_is_not_clamped():
    with pytest.raises(ValueError, match="score cannot exceed total"):
        _apply(score=3, total=2)


def test_string_score_is_not_coerced():
    with pytest.raises(ValueError, match="score must be a non-negative integer"):
        _apply(score="1")


def test_zero_total_is_not_clamped_to_one():
    with pytest.raises(ValueError, match="total must be between 1 and 100"):
        _apply(score=0, total=0)


def test_max_streak_cannot_exceed_total():
    with pytest.raises(ValueError, match="max_streak cannot exceed total"):
        _apply(max_streak=3, total=2)


def test_unknown_quiz_mode_is_rejected_before_receipt_creation():
    with pytest.raises(ValueError, match="unsupported quiz_mode"):
        _apply(quiz_mode="turbo")


def test_multiplier_cannot_exceed_product_ceiling():
    with pytest.raises(ValueError, match="at most 2"):
        _apply(score_multiplier=2.5)


def test_challenge_level_key_requires_challenge_mode():
    with pytest.raises(ValueError, match="requires challenge_mode"):
        _apply(level_key="random20", quiz_mode=None)


def test_challenge_level_key_must_match_mode():
    with pytest.raises(ValueError, match="must match challenge_mode"):
        _apply(
            level_key="hardcore20",
            challenge_mode="random20",
            quiz_mode=None,
        )


def test_challenge_cannot_carry_normal_quiz_mode():
    with pytest.raises(ValueError, match="cannot carry normal quiz_mode"):
        _apply(
            level_key="random20",
            challenge_mode="random20",
            quiz_mode="speed",
        )


def test_challenge_multiplier_must_remain_one():
    with pytest.raises(ValueError, match=r"multiplier must be 1\.0"):
        _apply(
            level_key="random20",
            challenge_mode="random20",
            quiz_mode=None,
            score_multiplier=2.0,
        )


def test_random_all_is_a_valid_level_bucket_not_a_persisted_session_mode():
    assert store._strict_level_key("random_all") == "random_all"
