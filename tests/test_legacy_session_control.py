import pytest

import legacy_session_control as control


def _partial():
    question = {"id": "q1", "question": "Q1"}
    return {
        "_id": "container-1",
        "attempt_id": "attempt-1",
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "level_key": "easy",
        "question_ids": ["q1", "q2"],
        "questions_data": [question, {"id": "q2", "question": "Q2"}],
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
        "start_time": 100.0,
    }


def _complete():
    question = {"id": "q1", "question": "Q1"}
    return {
        "_id": "container-1",
        "attempt_id": "attempt-1",
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
        "start_time": 100.0,
    }


def test_no_active_session_is_clean_noop(monkeypatch):
    monkeypatch.setattr(control, "get_active_quiz_session_strict", lambda _uid: None)
    monkeypatch.setattr(
        control,
        "cancel_owned_incomplete_quiz_attempt",
        lambda *_args, **_kwargs: pytest.fail("nothing active must not cancel"),
    )

    result = control.cancel_current_incomplete_session(42)

    assert result.had_active_session is False
    assert result.cancelled_now is False


def test_partial_attempt_is_cancelled_by_exact_attempt_identity(monkeypatch):
    session = _partial()
    captured = {}
    monkeypatch.setattr(control, "get_active_quiz_session_strict", lambda _uid: session)

    def cancel(session_id, user_id, *, expected_attempt_id):
        captured.update(
            session_id=session_id,
            user_id=user_id,
            expected_attempt_id=expected_attempt_id,
        )
        return {"applied": True, "session": {**session, "status": "cancelled"}}

    monkeypatch.setattr(control, "cancel_owned_incomplete_quiz_attempt", cancel)

    result = control.cancel_current_incomplete_session(42)

    assert captured == {
        "session_id": "container-1",
        "user_id": 42,
        "expected_attempt_id": "attempt-1",
    }
    assert result.had_active_session is True
    assert result.cancelled_now is True
    assert result.attempt_id == "attempt-1"


def test_completed_unscored_evidence_is_never_cancelled(monkeypatch):
    monkeypatch.setattr(control, "get_active_quiz_session_strict", lambda _uid: _complete())
    monkeypatch.setattr(
        control,
        "cancel_owned_incomplete_quiz_attempt",
        lambda *_args, **_kwargs: pytest.fail("completed evidence must not be cancelled"),
    )

    with pytest.raises(control.LegacySessionResultPending, match="finalized"):
        control.cancel_current_incomplete_session(42)


def test_store_outage_is_not_treated_as_no_active_session(monkeypatch):
    def fail(_uid):
        raise control.QuizSessionAccessUnavailable("mongo down")

    monkeypatch.setattr(control, "get_active_quiz_session_strict", fail)

    with pytest.raises(control.LegacySessionControlUnavailable, match="lookup"):
        control.cancel_current_incomplete_session(42)


def test_ambiguous_duplicate_active_lookup_is_control_conflict(monkeypatch):
    monkeypatch.setattr(
        control,
        "get_active_quiz_session_strict",
        lambda _uid: (_ for _ in ()).throw(
            control.QuizSessionAccessSchemaInvalid("multiple active sessions")
        ),
    )
    monkeypatch.setattr(
        control,
        "cancel_owned_incomplete_quiz_attempt",
        lambda *_args, **_kwargs: pytest.fail("ambiguous state must never cancel"),
    )

    with pytest.raises(control.LegacySessionControlConflict, match="ambiguous"):
        control.cancel_current_incomplete_session(42)


def test_contradictory_active_state_is_not_cancelled(monkeypatch):
    bad = _partial()
    bad["correct_count"] = "1"
    monkeypatch.setattr(control, "get_active_quiz_session_strict", lambda _uid: bad)
    monkeypatch.setattr(
        control,
        "cancel_owned_incomplete_quiz_attempt",
        lambda *_args, **_kwargs: pytest.fail("corrupt evidence must not be cancelled"),
    )

    with pytest.raises(control.LegacySessionControlConflict, match="contradictory"):
        control.cancel_current_incomplete_session(42)


def test_cancel_race_is_explicit(monkeypatch):
    session = _partial()
    monkeypatch.setattr(control, "get_active_quiz_session_strict", lambda _uid: session)

    def conflict(*_args, **_kwargs):
        raise control.QuizSessionLifecycleConflict("attempt changed")

    monkeypatch.setattr(control, "cancel_owned_incomplete_quiz_attempt", conflict)

    with pytest.raises(control.LegacySessionControlConflict, match="changed"):
        control.cancel_current_incomplete_session(42)
