from datetime import UTC, datetime

import pytest

from config import SPEED_MODE_TIMEOUT, TIMED_MODE_TIMEOUT
from legacy_session_recovery import (
    LegacyPersistedSessionModeInvalid,
    completed_result_inputs,
    persisted_result_time_seconds,
    recovery_fields,
    session_is_complete,
)


def _session(**overrides):
    base = {
        "_id": "session-1",
        "attempt_id": "attempt-1",
        "mode": "level",
        "question_ids": ["q1", "q2", "q3"],
        "questions_data": [{"id": "q1"}, {"id": "q2"}, {"id": "q3"}],
        "current_index": 2,
        "correct_count": 1,
        "answered_questions": [
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 1, "qid": "q2", "is_correct": False, "ts": "2026-08-10T12:00:20"},
        ],
        "level_key": "easy",
        "level_name": "Easy",
        "chat_id": 42,
        "start_time": datetime(2026, 8, 10, 12, 0, 0).timestamp(),
        "time_limit": None,
    }
    base.update(overrides)
    answered = base.get("answered_questions")
    question_ids = base.get("question_ids")
    if isinstance(answered, list):
        for index, item in enumerate(answered):
            if not isinstance(item, dict):
                continue
            item.setdefault("user_answer", f"answer-{index}")
            question_id = (
                question_ids[index]
                if isinstance(question_ids, list) and index < len(question_ids)
                else f"q{index + 1}"
            )
            item.setdefault("question_obj", {"id": question_id})
    return base


def test_recovery_preserves_explicit_attempt_identity():
    assert recovery_fields(_session())["attempt_id"] == "attempt-1"


def test_legacy_recovery_falls_back_to_session_container_id():
    session = _session()
    session.pop("attempt_id")
    assert recovery_fields(session)["attempt_id"] == "session-1"


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


def test_unknown_or_malformed_normal_timer_fails_closed():
    for value in (17, 0, "", str(TIMED_MODE_TIMEOUT), True):
        with pytest.raises(LegacyPersistedSessionModeInvalid, match="time_limit"):
            recovery_fields(_session(time_limit=value))


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


def test_malformed_challenge_timer_fails_closed():
    for value in (0, -1, "20", True):
        with pytest.raises(LegacyPersistedSessionModeInvalid, match="Challenge time_limit"):
            recovery_fields(
                _session(mode="random20", level_key="random20", time_limit=value)
            )


def test_recovery_rejects_string_or_boolean_persisted_counters():
    for field, value in (("current_index", "2"), ("correct_count", "1"), ("current_index", True)):
        with pytest.raises(LegacyPersistedSessionModeInvalid, match=field):
            recovery_fields(_session(**{field: value}))


def test_recovery_reconstructs_streaks_from_persisted_answers():
    fields = recovery_fields(
        _session(
            current_index=5,
            question_ids=[str(i) for i in range(6)],
            questions_data=[{"id": str(i)} for i in range(6)],
            answered_questions=[
                {"index": 0, "qid": "0", "is_correct": True, "ts": "2026-08-10T12:00:05"},
                {"index": 1, "qid": "1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
                {"index": 2, "qid": "2", "is_correct": False, "ts": "2026-08-10T12:00:15"},
                {"index": 3, "qid": "3", "is_correct": True, "ts": "2026-08-10T12:00:20"},
                {"index": 4, "qid": "4", "is_correct": True, "ts": "2026-08-10T12:00:25"},
            ],
        )
    )

    assert fields["current_streak"] == 2
    assert fields["max_streak"] == 2


def test_completed_session_is_result_pending_not_cancel_candidate():
    session = _session(current_index=3)

    assert session_is_complete(session) is True
    assert recovery_fields(session)["result_pending"] is True


def test_string_completed_index_is_not_completion_evidence():
    assert session_is_complete(_session(current_index="3")) is False


def test_overrun_session_is_contradictory_not_completed():
    session = _session(
        current_index=4,
        correct_count=2,
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 1, "qid": "q2", "is_correct": False, "ts": "2026-08-10T12:00:20"},
            {"index": 2, "qid": "q3", "is_correct": True, "ts": "2026-08-10T12:00:45"},
        ],
    )

    assert session_is_complete(session) is False
    assert recovery_fields(session)["result_pending"] is False
    assert completed_result_inputs(session) is None


def test_persisted_result_time_stops_at_last_answer_not_recovery_time():
    session = _session(
        current_index=3,
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 1, "qid": "q2", "is_correct": True, "ts": "2026-08-10T12:00:20"},
            {"index": 2, "qid": "q3", "is_correct": True, "ts": "2026-08-10T12:00:45"},
        ],
    )

    assert persisted_result_time_seconds(session) == 45.0
    assert recovery_fields(session)["persisted_result_time"] == 45.0


def test_offset_aware_answer_timestamp_is_converted_to_utc():
    started = datetime(2026, 8, 10, 9, 0, 0, tzinfo=UTC).timestamp()
    session = _session(
        start_time=started,
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:05+03:00"},
            {"index": 1, "qid": "q2", "is_correct": False, "ts": "2026-08-10T12:00:20+03:00"},
        ],
    )

    assert persisted_result_time_seconds(session) == 20.0


def test_answer_timestamp_before_start_is_rejected():
    session = _session(
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T11:59:59"},
        ]
    )

    assert persisted_result_time_seconds(session) is None


def test_non_monotonic_answer_timeline_is_rejected():
    session = _session(
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:20"},
            {"index": 1, "qid": "q2", "is_correct": False, "ts": "2026-08-10T12:00:10"},
        ]
    )

    assert persisted_result_time_seconds(session) is None


def test_fastest_answer_is_unknown_after_restart_without_latency_evidence():
    fields = recovery_fields(_session(time_limit=SPEED_MODE_TIMEOUT))

    assert fields["fastest_answer"] is None


def test_completed_result_inputs_use_only_persisted_score_total_duration_and_attempt():
    session = _session(
        current_index=3,
        correct_count=2,
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 1, "qid": "q2", "is_correct": False, "ts": "2026-08-10T12:00:20"},
            {"index": 2, "qid": "q3", "is_correct": True, "ts": "2026-08-10T12:00:45"},
        ],
    )

    result = completed_result_inputs(session)

    assert result is not None
    assert result["score"] == 2
    assert result["total"] == 3
    assert result["time_seconds"] == 45.0
    assert result["data"]["attempt_id"] == "attempt-1"
    assert result["data"]["result_pending"] is True


def test_completed_result_inputs_refuse_qid_or_index_mismatch():
    bad_qid = _session(
        current_index=3,
        correct_count=2,
        answered_questions=[
            {"index": 0, "qid": "q2", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 1, "qid": "q1", "is_correct": False, "ts": "2026-08-10T12:00:20"},
            {"index": 2, "qid": "q3", "is_correct": True, "ts": "2026-08-10T12:00:45"},
        ],
    )
    bad_index = _session(
        current_index=3,
        correct_count=2,
        answered_questions=[
            {"index": 1, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 0, "qid": "q2", "is_correct": False, "ts": "2026-08-10T12:00:20"},
            {"index": 2, "qid": "q3", "is_correct": True, "ts": "2026-08-10T12:00:45"},
        ],
    )

    assert completed_result_inputs(bad_qid) is None
    assert completed_result_inputs(bad_index) is None


def test_completed_result_inputs_refuse_missing_qid():
    session = _session(
        current_index=3,
        correct_count=2,
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 1, "is_correct": False, "ts": "2026-08-10T12:00:20"},
            {"index": 2, "qid": "q3", "is_correct": True, "ts": "2026-08-10T12:00:45"},
        ],
    )

    assert completed_result_inputs(session) is None


def test_completed_result_inputs_refuse_missing_answer_payload():
    missing_user_answer = _session(
        current_index=3,
        correct_count=2,
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 1, "qid": "q2", "is_correct": False, "ts": "2026-08-10T12:00:20"},
            {"index": 2, "qid": "q3", "is_correct": True, "ts": "2026-08-10T12:00:45"},
        ],
    )
    missing_user_answer["answered_questions"][1].pop("user_answer")

    missing_question_obj = _session(
        current_index=3,
        correct_count=2,
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 1, "qid": "q2", "is_correct": False, "ts": "2026-08-10T12:00:20"},
            {"index": 2, "qid": "q3", "is_correct": True, "ts": "2026-08-10T12:00:45"},
        ],
    )
    missing_question_obj["answered_questions"][1].pop("question_obj")

    assert completed_result_inputs(missing_user_answer) is None
    assert completed_result_inputs(missing_question_obj) is None


def test_completed_result_inputs_refuse_missing_last_answer_timestamp():
    session = _session(
        current_index=3,
        correct_count=2,
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 1, "qid": "q2", "is_correct": False, "ts": "2026-08-10T12:00:20"},
            {"index": 2, "qid": "q3", "is_correct": True},
        ],
    )

    assert completed_result_inputs(session) is None


def test_completed_result_inputs_refuse_incomplete_session():
    assert completed_result_inputs(_session(current_index=2)) is None


def test_completed_result_inputs_refuse_missing_answer_record():
    session = _session(
        current_index=3,
        correct_count=1,
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 1, "qid": "q2", "is_correct": False, "ts": "2026-08-10T12:00:20"},
        ],
    )

    assert completed_result_inputs(session) is None


def test_completed_result_inputs_refuse_correct_counter_mismatch():
    session = _session(
        current_index=3,
        correct_count=3,
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 1, "qid": "q2", "is_correct": False, "ts": "2026-08-10T12:00:20"},
            {"index": 2, "qid": "q3", "is_correct": True, "ts": "2026-08-10T12:00:45"},
        ],
    )

    assert completed_result_inputs(session) is None


def test_completed_result_inputs_refuse_non_boolean_correctness():
    session = _session(
        current_index=3,
        correct_count=2,
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:10"},
            {"index": 1, "qid": "q2", "is_correct": "false", "ts": "2026-08-10T12:00:20"},
            {"index": 2, "qid": "q3", "is_correct": True, "ts": "2026-08-10T12:00:45"},
        ],
    )

    assert completed_result_inputs(session) is None
