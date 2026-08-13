import math

import pytest

import legacy_session_recovery as recovery


def _complete(start_time):
    question = {"id": "q1"}
    return {
        "_id": "legacy-session",
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "level_key": "easy",
        "question_ids": ["q1"],
        "questions_data": [question],
        "current_index": 1,
        "correct_count": 1,
        "answered_questions": [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": question,
                "ts": "1970-01-01T00:01:41",
            }
        ],
        "time_limit": None,
        "start_time": start_time,
    }


def test_completed_result_accepts_real_numeric_start_time():
    session = _complete(100.0)

    assert recovery.persisted_result_time_seconds(session) == 1.0
    result = recovery.completed_result_inputs(session)

    assert result is not None
    assert result["score"] == 1
    assert result["total"] == 1
    assert result["time_seconds"] == 1.0
    assert result["completed_at"] == "1970-01-01T00:01:41"


@pytest.mark.parametrize(
    "start_time",
    [
        "100",
        True,
        False,
        None,
        -1,
        float("nan"),
        float("inf"),
        float("-inf"),
    ],
)
def test_completed_result_rejects_coerced_or_nonfinite_start_time(start_time):
    session = _complete(start_time)

    assert recovery.persisted_result_time_seconds(session) is None
    assert recovery.persisted_completed_at(session) is None
    assert recovery.completed_result_inputs(session) is None


def test_completed_result_rejects_finite_timestamp_outside_datetime_range():
    session = _complete(10**30)

    assert math.isfinite(float(session["start_time"]))
    assert recovery.persisted_result_time_seconds(session) is None
    assert recovery.completed_result_inputs(session) is None
