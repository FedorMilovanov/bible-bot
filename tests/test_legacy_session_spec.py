import pytest

from config import SPEED_MODE_TIMEOUT, TIMED_MODE_TIMEOUT
from legacy_session_spec import validated_session_spec


def _spec(**overrides):
    kwargs = {
        "mode": "level",
        "question_ids": ["q1"],
        "questions_data": [{"question": "Q1"}],
        "level_key": "easy",
        "level_name": "Easy",
        "time_limit": None,
        "chat_id": 42,
    }
    kwargs.update(overrides)
    return validated_session_spec(**kwargs)


@pytest.mark.parametrize("time_limit", [None, TIMED_MODE_TIMEOUT, SPEED_MODE_TIMEOUT])
def test_normal_level_accepts_only_product_timer_modes(time_limit):
    result = _spec(time_limit=time_limit)
    assert result["mode"] == "level"
    assert result["level_key"] == "easy"
    assert result["time_limit"] == time_limit


@pytest.mark.parametrize("time_limit", [1, 10, 45])
def test_challenge_keeps_positive_legacy_timer_compatibility(time_limit):
    result = _spec(
        mode="hardcore20",
        level_key="hardcore20",
        time_limit=time_limit,
    )
    assert result["mode"] == "hardcore20"
    assert result["level_key"] == "hardcore20"
    assert result["time_limit"] == time_limit


def test_normal_level_rejects_missing_or_challenge_level_key():
    with pytest.raises(ValueError, match="normal level_key"):
        _spec(level_key=None)
    with pytest.raises(ValueError, match="normal level_key"):
        _spec(level_key="random20")


def test_normal_level_rejects_timer_recovery_would_not_recognize():
    with pytest.raises(ValueError, match="recognized product timer"):
        _spec(time_limit=17)


def test_challenge_level_key_must_match_mode_or_be_absent():
    assert _spec(mode="random20", level_key=None, time_limit=10)["level_key"] is None
    with pytest.raises(ValueError, match="Challenge level_key"):
        _spec(mode="random20", level_key="easy", time_limit=10)
    with pytest.raises(ValueError, match="Challenge level_key"):
        _spec(mode="random20", level_key="hardcore20", time_limit=10)
