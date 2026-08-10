import pytest

from config import SPEED_MODE_TIMEOUT, TIMED_MODE_TIMEOUT
from legacy_session_recovery import LegacyPersistedSessionModeInvalid, recovery_fields


def test_normal_recovery_accepts_only_known_timer_semantics():
    assert recovery_fields({"mode": "level", "time_limit": None})["quiz_mode"] == "relaxed"
    assert recovery_fields({"mode": "level", "time_limit": TIMED_MODE_TIMEOUT})["quiz_mode"] == "timed"
    assert recovery_fields({"mode": "level", "time_limit": SPEED_MODE_TIMEOUT})["quiz_mode"] == "speed"


def test_normal_recovery_rejects_ambiguous_or_malformed_timer():
    for value in (0, -1, True, "30", 999999):
        with pytest.raises(LegacyPersistedSessionModeInvalid, match="time_limit"):
            recovery_fields({"mode": "level", "time_limit": value})


def test_challenge_recovery_accepts_positive_integer_or_no_timer():
    assert recovery_fields({"mode": "random20", "time_limit": 20})["challenge_time_limit"] == 20
    assert recovery_fields({"mode": "hardcore20", "time_limit": None})["challenge_time_limit"] is None


def test_challenge_recovery_rejects_malformed_timer():
    for value in (0, -5, True, "20"):
        with pytest.raises(LegacyPersistedSessionModeInvalid, match="time_limit"):
            recovery_fields({"mode": "random20", "time_limit": value})
