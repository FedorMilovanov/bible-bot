from datetime import datetime

from config import SPEED_MODE_TIMEOUT, TIMED_MODE_TIMEOUT
from legacy_session_recovery import (
    completed_result_inputs,
    persisted_result_time_seconds,
    recovery_fields,
    session_is_complete,
)


def _session(**overrides):
    base = {
        "_id": "session-1",
        "mode": "level",
        "questions_data": [{"id": "q1"}, {"id": "q2"}, {"id": "q3"}],
        "current_index": 2,
        "correct_count": 1,
        "answered_questions": [
            {"is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"is_correct": False, "ts": "2026-08-10T12:00:20"},
        ],
        "level_key": "easy",
        "level_name": "Easy",
        "chat_id": 42,
        "start_time": datetime(2026, 8, 10, 12, 0, 0).timestamp(),
        "time_limit": None,
    }
    base.update(overrides)
    return base


def test_timed_session_restores_timer_mode_and_multiplier():
    fields = recovery_fields(_session(time_limit=TIMED_MODE_TIMEOUT))

    assert fields["is_challenge"] is False
    assert fields["quiz_mode"] == "timed"
    assert fields["score_multiplier"] == 1.5
    assert fields["quiz_time_limit"] == TIMED_MODE_TIMEOUT


def test_speed_session_restores_speed_multiplier():
    fields = recovery_fields(_session(time_limit=SPEED_MODE_TIMEOUT))

    assert fields["quiz_mode"] == "speed"
    assert fields["score_multiplier"] == 2.0
    assert fields["quiz_time_limit"] == SPEED_MODE_TIMEOUT


def test_unknown_legacy_timer_never_invents_bonus_multiplier():
    fields = recovery_fields(_session(time_limit=17))

    assert fields["quiz_mode"] == "timed"
    assert fields["score_multiplier"] == 1.0
    assert fields["quiz_time_limit"] == 17


def test_hardcore_session_restores_challenge_timer_not_normal_timer():
    fields = recovery_fields(
        _session(mode="hardcore20", level_key="hardcore20", time_limit=10)
    )

    assert fields["is_challenge"] is True
    assert fields["challenge_mode"] == "hardcore20"
    assert fields["challenge_time_limit"] == 10
    assert fields["quiz_mode"] is None
    assert fields["quiz_time_limit"] is None
    assert fields["score_multiplier"] == 1.0


def test_recovery_reconstructs_streaks_from_persisted_answers():
    fields = recovery_fields(
        _session(
            current_index=5,
            questions_data=[{"id": str(i)} for i in range(6)],
            answered_questions=[
                {"is_correct": True, "ts": "2026-08-10T12:00:05"},
                {"is_correct": True, "ts": "2026-08-10T12:00:10"},
                {"is_correct": False, "ts": "2026-08-10T12:00:15"},
                {"is_correct": True, "ts": "2026-08-10T12:00:20"},
                {"is_correct": True, "ts": "2026-08-10T12:00:25"},
            ],
        )
    )

    assert fields["current_streak"] == 2
    assert fields["max_streak"] == 2


def test_completed_session_is_result_pending_not_cancel_candidate():
    session = _session(current_index=3)

    assert session_is_complete(session) is True
    assert recovery_fields(session)["result_pending"] is True


def test_persisted_result_time_stops_at_last_answer_not_recovery_time():
    session = _session(
        current_index=3,
        answered_questions=[
            {"is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"is_correct": True, "ts": "2026-08-10T12:00:20"},
            {"is_correct": True, "ts": "2026-08-10T12:00:45"},
        ],
    )

    assert persisted_result_time_seconds(session) == 45.0
    assert recovery_fields(session)["persisted_result_time"] == 45.0


def test_fastest_answer_is_unknown_after_restart_without_latency_evidence():
    fields = recovery_fields(_session(time_limit=SPEED_MODE_TIMEOUT))

    assert fields["fastest_answer"] is None


def test_completed_result_inputs_use_only_persisted_score_total_and_duration():
    session = _session(
        current_index=3,
        correct_count=2,
        answered_questions=[
            {"is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"is_correct": False, "ts": "2026-08-10T12:00:20"},
            {"is_correct": True, "ts": "2026-08-10T12:00:45"},
        ],
    )

    result = completed_result_inputs(session)

    assert result is not None
    assert result["score"] == 2
    assert result["total"] == 3
    assert result["time_seconds"] == 45.0
    assert result["data"]["result_pending"] is True


def test_completed_result_inputs_refuse_missing_last_answer_timestamp():
    session = _session(
        current_index=3,
        correct_count=2,
        answered_questions=[
            {"is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"is_correct": False, "ts": "2026-08-10T12:00:20"},
            {"is_correct": True},
        ],
    )

    assert completed_result_inputs(session) is None


def test_completed_result_inputs_refuse_incomplete_session():
    assert completed_result_inputs(_session(current_index=2)) is None
