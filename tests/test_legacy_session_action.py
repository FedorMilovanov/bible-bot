import pytest

import legacy_session_action as actions

_SESSION = "12345678-1234-5678-9234-567812345678"


def _session(*, attempt_id="attempt-1", current=0):
    answered = []
    correct = 0
    if current:
        answered = [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": {"id": "q1"},
                "ts": "2026-08-10T12:00:01",
            }
        ]
        correct = 1
    return {
        "_id": _SESSION,
        "attempt_id": attempt_id,
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "level_key": "easy",
        "question_ids": ["q1", "q2"],
        "questions_data": [{"id": "q1"}, {"id": "q2"}],
        "current_index": current,
        "correct_count": correct,
        "answered_questions": answered,
        "time_limit": None,
    }


def test_payloads_bind_all_actions_to_same_attempt():
    payloads = actions.session_action_payloads(_session())
    assert set(payloads) == {"res", "rst", "can"}
    assert all(_SESSION in payload for payload in payloads.values())


def test_resolver_uses_owner_scoped_store_and_attempt_token(monkeypatch):
    session = _session(current=1)
    payload = actions.session_action_payloads(session)["res"]
    captured = {}

    def get(session_id, *, user_id):
        captured.update(session_id=session_id, user_id=user_id)
        return session

    monkeypatch.setattr(actions, "get_quiz_session_strict", get)

    result = actions.resolve_session_action(payload, "res", 42)

    assert captured == {"session_id": _SESSION, "user_id": 42}
    assert result.session_id == _SESSION
    assert result.attempt_id == "attempt-1"
    assert result.decision.action == "resume"


def test_old_button_is_stale_after_same_container_restart(monkeypatch):
    old = _session(attempt_id="attempt-old")
    payload = actions.session_action_payloads(old)["can"]
    monkeypatch.setattr(
        actions,
        "get_quiz_session_strict",
        lambda *_args, **_kwargs: _session(attempt_id="attempt-new"),
    )

    with pytest.raises(actions.LegacySessionActionStale, match="another attempt"):
        actions.resolve_session_action(payload, "can", 42)


def test_missing_or_unowned_session_is_stale(monkeypatch):
    payload = actions.session_action_payloads(_session())["rst"]
    monkeypatch.setattr(actions, "get_quiz_session_strict", lambda *_args, **_kwargs: None)

    with pytest.raises(actions.LegacySessionActionStale, match="missing or not owned"):
        actions.resolve_session_action(payload, "rst", 42)


def test_completed_button_resolves_to_finalize_not_cancel(monkeypatch):
    session = {
        "_id": _SESSION,
        "attempt_id": "attempt-1",
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "level_key": "easy",
        "question_ids": ["q1"],
        "questions_data": [{"id": "q1"}],
        "current_index": 1,
        "correct_count": 1,
        "answered_questions": [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": {"id": "q1"},
                "ts": "2026-08-10T12:00:01",
            }
        ],
        "time_limit": None,
        "start_time": 0.0,
    }
    payload = actions.session_action_payloads(session)["can"]
    monkeypatch.setattr(actions, "get_quiz_session_strict", lambda *_args, **_kwargs: session)

    result = actions.resolve_session_action(payload, "can", 42)
    assert result.decision.action == "finalize"
