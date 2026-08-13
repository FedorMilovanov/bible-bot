import pytest

from legacy_restart_policy import LegacyRestartStateInvalid, classify_restart_session


def _partial(*, current=0, start_time=100.0, answered=None):
    questions = [
        {"id": "q1", "question": "Q1"},
        {"id": "q2", "question": "Q2"},
    ]
    if answered is None:
        answered = [] if current == 0 else [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": questions[0],
                "ts": "1970-01-01T00:01:41",
            }
        ]
    return {
        "_id": "s1",
        "attempt_id": "a1",
        "status": "in_progress",
        "mode": "level",
        "level_key": "easy",
        "question_ids": ["q1", "q2"],
        "questions_data": questions,
        "current_index": current,
        "correct_count": 1 if current else 0,
        "answered_questions": answered,
        "time_limit": None,
        "start_time": start_time,
    }


def test_zero_progress_resume_requires_valid_start_time():
    for value in (None, "100", True, -1.0, float("inf")):
        with pytest.raises(LegacyRestartStateInvalid, match="start_time"):
            classify_restart_session(_partial(start_time=value))


def test_partial_answer_chronology_must_remain_recoverable():
    bad = _partial(
        current=1,
        answered=[
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": {"id": "q1", "question": "Q1"},
                "ts": "1970-01-01T00:01:39",
            }
        ],
    )

    with pytest.raises(LegacyRestartStateInvalid, match="chronology"):
        classify_restart_session(bad)


def test_valid_partial_chronology_remains_resumable():
    decision = classify_restart_session(_partial(current=1))
    assert decision.action == "resume"
    assert decision.current_index == 1
