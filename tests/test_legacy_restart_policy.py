from datetime import datetime

import pytest

from config import SPEED_MODE_TIMEOUT, TIMED_MODE_TIMEOUT
from legacy_restart_policy import (
    LegacyRestartStateInvalid,
    classify_restart_session,
    restart_timeout_route,
)


def session(mode="level", current=0, time_limit=None):
    questions = [
        {"id": "q0", "question": "Q0", "options": ["A", "B"], "correct": 0},
        {"id": "q1", "question": "Q1", "options": ["A", "B"], "correct": 0},
    ]
    answered = []
    if current >= 1:
        answered.append({
            "index": 0, "qid": "q0", "user_answer": "A",
            "is_correct": True, "question_obj": questions[0],
            "latency_seconds": 2.0,
            "ts": datetime.utcfromtimestamp(101).isoformat(),
        })
    if current >= 2:
        answered.append({
            "index": 1, "qid": "q1", "user_answer": "B",
            "is_correct": False, "question_obj": questions[1],
            "latency_seconds": 3.0,
            "ts": datetime.utcfromtimestamp(102).isoformat(),
        })
    return {
        "_id": "s1", "user_id": "42", "status": "in_progress",
        "mode": mode, "level_key": "easy" if mode == "level" else mode,
        "level_name": "Test", "question_ids": ["q0", "q1"],
        "questions_data": questions, "current_index": current,
        "correct_count": 1 if current else 0,
        "answered_questions": answered, "time_limit": time_limit,
        "start_time": 100.0, "chat_id": 100,
    }


def test_partial_session_resumes_but_exact_completion_finalizes():
    partial = classify_restart_session(session(current=1))
    complete = classify_restart_session(session(current=2))

    assert (partial.action, partial.current_index, partial.total) == ("resume", 1, 2)
    assert complete.action == "finalize"
    assert complete.result_inputs["score"] == 1
    assert complete.result_inputs["completed_at"] == datetime.utcfromtimestamp(102).isoformat()


def test_incomplete_completion_evidence_is_conflict_not_cancel():
    value = session(current=2)
    value["answered_questions"] = value["answered_questions"][:1]

    with pytest.raises(LegacyRestartStateInvalid, match="scoring evidence"):
        classify_restart_session(value)


def test_index_overrun_and_unknown_mode_fail_closed():
    with pytest.raises(LegacyRestartStateInvalid, match="exceeds"):
        classify_restart_session(session(current=3))
    with pytest.raises(LegacyRestartStateInvalid, match="mode is invalid"):
        classify_restart_session(session(mode="other", current=1))


def test_normal_timers_route_to_normal_handler():
    timed = restart_timeout_route(session(time_limit=TIMED_MODE_TIMEOUT))
    speed = restart_timeout_route(session(time_limit=SPEED_MODE_TIMEOUT))

    assert (timed.route, timed.time_limit) == ("normal", TIMED_MODE_TIMEOUT)
    assert (speed.route, speed.time_limit) == ("normal", SPEED_MODE_TIMEOUT)


def test_challenge_timer_routes_only_to_challenge_handler():
    route = restart_timeout_route(session(mode="random20", time_limit=20))
    assert (route.route, route.time_limit) == ("challenge", 20)


def test_untimed_session_has_no_restart_timeout():
    normal = restart_timeout_route(session())
    challenge = restart_timeout_route(session(mode="hardcore20"))
    assert (normal.route, normal.time_limit) == ("none", None)
    assert (challenge.route, challenge.time_limit) == ("none", None)
