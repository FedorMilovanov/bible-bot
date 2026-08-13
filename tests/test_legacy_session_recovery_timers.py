import pytest

from config import SPEED_MODE_TIMEOUT, TIMED_MODE_TIMEOUT
from legacy_session_recovery import LegacyPersistedSessionModeInvalid, recovery_fields


def _session(mode: str, time_limit):
    return {"_id": "legacy-session", "mode": mode, "time_limit": time_limit}


def test_normal_recovery_accepts_only_known_timer_semantics():
    assert recovery_fields(_session("level", None))["quiz_mode"] == "relaxed"
    assert recovery_fields(_session("level", TIMED_MODE_TIMEOUT))["quiz_mode"] == "timed"
    assert recovery_fields(_session("level", SPEED_MODE_TIMEOUT))["quiz_mode"] == "speed"


def test_normal_recovery_rejects_ambiguous_or_malformed_timer():
    for value in (0, -1, True, "30", 999999):
        with pytest.raises(LegacyPersistedSessionModeInvalid, match="time_limit"):
            recovery_fields(_session("level", value))


def test_challenge_recovery_accepts_positive_integer_or_no_timer():
    assert recovery_fields(_session("random20", 20))["challenge_time_limit"] == 20
    assert recovery_fields(_session("hardcore20", None))["challenge_time_limit"] is None


def test_challenge_recovery_rejects_malformed_timer():
    for value in (0, -5, True, "20"):
        with pytest.raises(LegacyPersistedSessionModeInvalid, match="time_limit"):
            recovery_fields(_session("random20", value))
