import pytest

import legacy_live_finalize as live


def _session(*, attempt_id="attempt-1", complete=True, challenge=False):
    questions = [{"id": "q1"}, {"id": "q2"}]
    answered = [
        {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:05"},
        {"index": 1, "qid": "q2", "is_correct": False, "ts": "2026-08-10T12:00:20"},
    ]
    if not complete:
        answered = answered[:1]
    return {
        "_id": "container-1",
        "attempt_id": attempt_id,
        "user_id": "42",
        "status": "in_progress",
        "mode": "random20" if challenge else "level",
        "level_key": "random20" if challenge else "easy",
        "level_name": "Test",
        "question_ids": ["q1", "q2"],
        "questions_data": questions,
        "current_index": 2 if complete else 1,
        "correct_count": 1,
        "answered_questions": answered,
        "time_limit": 20 if challenge else None,
        "start_time": 1754827200.0,
    }


def _data(*, attempt_id="attempt-1"):
    return {
        "session_id": "container-1",
        "attempt_id": attempt_id,
        "correct_answers": 999,
        "current_question": 999,
        "start_time": 0,
    }


def test_live_finalization_uses_durable_score_total_and_time_not_ram(monkeypatch):
    session = _session()
    captured = {}
    monkeypatch.setattr(live, "get_quiz_session_strict", lambda *_args, **_kwargs: session)
    monkeypatch.setattr(live, "completed_result_inputs", lambda value: {
        "score": 1, "total": 2, "time_seconds": 20.0,
        "completed_at": "2026-08-10T12:00:20", "data": {"is_challenge": False},
    })
    monkeypatch.setattr(live, "finalize_completed_session", lambda **kwargs: captured.update(kwargs) or {"scored": True})
    result = live.finalize_live_persisted_attempt(
        user_id=42, data=_data(), username="u", first_name="User", achievement_rewards={}
    )
    assert (result.score, result.total, result.time_seconds) == (1, 2, 20.0)
    assert result.attempt_id == "attempt-1"
    assert captured["session"] is session


def test_stale_ram_attempt_cannot_finalize_restarted_container(monkeypatch):
    monkeypatch.setattr(live, "get_quiz_session_strict", lambda *_args, **_kwargs: _session(attempt_id="attempt-new"))
    monkeypatch.setattr(live, "finalize_completed_session", lambda **_: pytest.fail("stale attempt must not score"))
    with pytest.raises(live.LegacyLiveFinalizationPending, match="different durable"):
        live.finalize_live_persisted_attempt(
            user_id=42, data=_data(attempt_id="attempt-old"), username="u", first_name="User", achievement_rewards={}
        )


def test_incomplete_durable_session_stays_pending(monkeypatch):
    monkeypatch.setattr(live, "get_quiz_session_strict", lambda *_args, **_kwargs: _session(complete=False))
    monkeypatch.setattr(live, "completed_result_inputs", lambda _session: None)
    monkeypatch.setattr(live, "finalize_completed_session", lambda **_: pytest.fail("incomplete result must not finalize"))
    with pytest.raises(live.LegacyLiveFinalizationPending, match="not exactly complete"):
        live.finalize_live_persisted_attempt(
            user_id=42, data=_data(), username=None, first_name=None, achievement_rewards={}
        )


def test_session_lookup_outage_is_retryable(monkeypatch):
    def fail(*_args, **_kwargs):
        raise live.QuizSessionAccessUnavailable("mongo down")
    monkeypatch.setattr(live, "get_quiz_session_strict", fail)
    with pytest.raises(live.LegacyLiveFinalizationPending, match="lookup"):
        live.finalize_live_persisted_attempt(
            user_id=42, data=_data(), username=None, first_name=None, achievement_rewards={}
        )


def test_challenge_flag_comes_from_durable_data(monkeypatch):
    session = _session(challenge=True)
    monkeypatch.setattr(live, "get_quiz_session_strict", lambda *_args, **_kwargs: session)
    monkeypatch.setattr(live, "completed_result_inputs", lambda _session: {
        "score": 1, "total": 2, "time_seconds": 20.0,
        "completed_at": "2026-08-10T12:00:20", "data": {"is_challenge": True},
    })
    monkeypatch.setattr(live, "finalize_completed_session", lambda **_: {"scored": True})
    result = live.finalize_live_persisted_attempt(
        user_id=42, data=_data(), username="u", first_name="User", achievement_rewards={}
    )
    assert result.is_challenge is True


def test_programming_error_is_not_hidden_as_retry(monkeypatch):
    session = _session()
    monkeypatch.setattr(live, "get_quiz_session_strict", lambda *_args, **_kwargs: session)
    monkeypatch.setattr(live, "completed_result_inputs", lambda _session: {
        "score": 1, "total": 2, "time_seconds": 20.0,
        "completed_at": "2026-08-10T12:00:20", "data": {"is_challenge": False},
    })
    monkeypatch.setattr(live, "finalize_completed_session", lambda **_: (_ for _ in ()).throw(ValueError("bug")))
    with pytest.raises(ValueError, match="bug"):
        live.finalize_live_persisted_attempt(
            user_id=42, data=_data(), username="u", first_name="User", achievement_rewards={}
        )
