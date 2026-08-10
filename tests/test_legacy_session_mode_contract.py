from datetime import datetime

import pytest

import legacy_session_finalize as finalize
from legacy_session_recovery import (
    LegacyPersistedSessionModeInvalid,
    recovery_fields,
)


def _completed_session(**overrides):
    session = {
        "_id": "session-mode-1",
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "questions_data": [{"id": "q1"}],
        "current_index": 1,
        "correct_count": 1,
        "answered_questions": [
            {"is_correct": True, "ts": "2026-08-10T12:00:05"},
        ],
        "level_key": "easy",
        "level_name": "Easy",
        "chat_id": 42,
        "start_time": datetime(2026, 8, 10, 12, 0, 0).timestamp(),
        "time_limit": None,
    }
    session.update(overrides)
    return session


@pytest.mark.parametrize("mode", ["level", "random20", "hardcore20"])
def test_known_persisted_quiz_modes_are_recoverable(mode):
    fields = recovery_fields(
        _completed_session(
            mode=mode,
            level_key=mode if mode != "level" else "easy",
        )
    )

    assert fields["is_challenge"] is (mode in {"random20", "hardcore20"})


@pytest.mark.parametrize("mode", [None, "", "random_all", "battle", "garbage"])
def test_unknown_persisted_quiz_mode_is_rejected(mode):
    with pytest.raises(LegacyPersistedSessionModeInvalid):
        recovery_fields(_completed_session(mode=mode))


def test_completed_finalizer_translates_unknown_mode_to_incomplete_evidence(monkeypatch):
    monkeypatch.setattr(
        finalize,
        "finalize_normal_result",
        lambda **_: pytest.fail("unknown mode must fail before normal scoring"),
    )
    monkeypatch.setattr(
        finalize,
        "finalize_challenge_result",
        lambda **_: pytest.fail("unknown mode must fail before Challenge scoring"),
    )

    with pytest.raises(
        finalize.LegacyCompletedSessionEvidenceIncomplete,
        match="unsupported persisted quiz mode",
    ):
        finalize.finalize_completed_session(
            user_id=42,
            session=_completed_session(mode="garbage"),
            username="u",
            first_name="User",
            achievement_rewards={},
        )
