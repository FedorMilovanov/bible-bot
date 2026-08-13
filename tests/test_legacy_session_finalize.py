from datetime import datetime

import pytest

import legacy_session_finalize as recovery


def _session(**overrides):
    questions = [{"id": "q1"}, {"id": "q2"}]
    session = {
        "_id": "session-1",
        "attempt_id": "attempt-1",
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "question_ids": ["q1", "q2"],
        "questions_data": questions,
        "current_index": 2,
        "correct_count": 1,
        "answered_questions": [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": questions[0],
                "ts": "2026-08-10T12:00:05",
            },
            {
                "index": 1,
                "qid": "q2",
                "user_answer": "B",
                "is_correct": False,
                "question_obj": questions[1],
                "ts": "2026-08-10T12:00:20",
            },
        ],
        "level_key": "easy",
        "level_name": "Easy",
        "chat_id": 42,
        "start_time": datetime(2026, 8, 10, 12, 0, 0).timestamp(),
        "time_limit": None,
        "is_retry": False,
    }
    session.update(overrides)
    return session


def test_completed_normal_session_uses_persisted_result_inputs(monkeypatch):
    captured = {}

    def normal(**kwargs):
        captured.update(kwargs)
        return {"scored": True, "result_id": "quiz:attempt-1"}

    monkeypatch.setattr(recovery, "finalize_normal_result", normal)
    monkeypatch.setattr(
        recovery,
        "finalize_challenge_result",
        lambda **_: pytest.fail("normal session must not use Challenge finalizer"),
    )

    result = recovery.finalize_completed_session(
        user_id=42,
        session=_session(),
        username="tester",
        first_name="Test",
        achievement_rewards={"first_steps": 10},
    )

    assert result["scored"] is True
    assert captured["score"] == 1
    assert captured["total"] == 2
    assert captured["time_seconds"] == 20.0
    assert captured["data"]["session_id"] == "session-1"
    assert captured["data"]["attempt_id"] == "attempt-1"
    assert captured["data"]["username"] == "tester"
    assert captured["data"]["first_name"] == "Test"
    assert captured["data"]["result_pending"] is True
    assert captured["data"]["result_completed_at"] == "2026-08-10T12:00:20"
    assert captured["data"]["is_retry"] is False


def test_completed_retry_practice_closes_without_entering_scoring(monkeypatch):
    closed = []
    monkeypatch.setattr(
        recovery,
        "finalize_normal_result",
        lambda **_: pytest.fail("retry practice must never enter normal scoring"),
    )
    monkeypatch.setattr(
        recovery,
        "finalize_challenge_result",
        lambda **_: pytest.fail("retry practice must never enter Challenge scoring"),
    )
    monkeypatch.setattr(
        recovery,
        "finish_completed_owned_quiz_session",
        lambda session_id, user_id: closed.append((session_id, user_id)) or {"status": "finished"},
    )

    result = recovery.finalize_completed_session(
        user_id=42,
        session=_session(is_retry=True),
        username="tester",
        first_name="Test",
        achievement_rewards={"first_steps": 10},
    )

    assert closed == [("session-1", 42)]
    assert result == {
        "scored": False,
        "practice": True,
        "earned_base": 0,
        "daily_bonus": {"bonus": 0, "eligible": False, "claimed_now": False},
        "new_achievements": [],
        "session_finished": True,
    }


def test_retry_practice_requires_exact_completion_before_close(monkeypatch):
    monkeypatch.setattr(
        recovery,
        "finish_completed_owned_quiz_session",
        lambda *_args: pytest.fail("incomplete retry practice must not close"),
    )

    with pytest.raises(recovery.LegacyCompletedSessionEvidenceIncomplete):
        recovery.finalize_completed_session(
            user_id=42,
            session=_session(is_retry=True, current_index=1),
            username="tester",
            first_name="Test",
            achievement_rewards={},
        )


def test_retry_policy_rejects_non_boolean_or_challenge_evidence(monkeypatch):
    monkeypatch.setattr(
        recovery,
        "finish_completed_owned_quiz_session",
        lambda *_args: pytest.fail("invalid retry policy must fail before close"),
    )

    for session in (
        _session(is_retry="yes"),
        _session(is_retry=True, mode="hardcore20", level_key="hardcore20", time_limit=10),
    ):
        with pytest.raises(recovery.LegacyCompletedSessionEvidenceIncomplete):
            recovery.finalize_completed_session(
                user_id=42,
                session=session,
                username="tester",
                first_name="Test",
                achievement_rewards={},
            )


def test_legacy_session_without_attempt_id_recovers_with_container_identity(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        recovery,
        "finalize_normal_result",
        lambda **kwargs: captured.update(kwargs) or {"scored": True},
    )
    session = _session()
    session.pop("attempt_id")

    recovery.finalize_completed_session(
        user_id=42,
        session=session,
        username="u",
        first_name="User",
        achievement_rewards={},
    )

    assert captured["data"]["attempt_id"] == "session-1"


def test_finished_completed_session_remains_recoverable_for_legacy_crash_boundary(monkeypatch):
    captured = {}

    def normal(**kwargs):
        captured.update(kwargs)
        return {"scored": True}

    monkeypatch.setattr(recovery, "finalize_normal_result", normal)

    result = recovery.finalize_completed_session(
        user_id=42,
        session=_session(status="finished"),
        username="u",
        first_name="User",
        achievement_rewards={},
    )

    assert result["scored"] is True
    assert captured["data"]["session_id"] == "session-1"
    assert captured["data"]["attempt_id"] == "attempt-1"


def test_completed_challenge_session_routes_to_challenge_finalizer(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        recovery,
        "finalize_normal_result",
        lambda **_: pytest.fail("Challenge session must not use normal finalizer"),
    )

    def challenge(**kwargs):
        captured.update(kwargs)
        return {"scored": True, "result_id": "quiz:attempt-1"}

    monkeypatch.setattr(recovery, "finalize_challenge_result", challenge)

    recovery.finalize_completed_session(
        user_id=42,
        session=_session(
            mode="hardcore20",
            level_key="hardcore20",
            time_limit=10,
        ),
        username=None,
        first_name=None,
        achievement_rewards={},
    )

    assert captured["data"]["is_challenge"] is True
    assert captured["data"]["challenge_mode"] == "hardcore20"
    assert captured["data"]["challenge_time_limit"] == 10
    assert captured["data"]["attempt_id"] == "attempt-1"
    assert captured["data"]["first_name"] == "Игрок"
    assert captured["data"]["result_completed_at"] == "2026-08-10T12:00:20"


def test_completed_session_recovery_rejects_other_owner(monkeypatch):
    monkeypatch.setattr(
        recovery,
        "finalize_normal_result",
        lambda **_: pytest.fail("owner mismatch must fail before scoring"),
    )

    with pytest.raises(recovery.LegacyCompletedSessionOwnerMismatch):
        recovery.finalize_completed_session(
            user_id=42,
            session=_session(user_id="99"),
            username="u",
            first_name="User",
            achievement_rewards={},
        )


@pytest.mark.parametrize("status", ["cancelled", "", None, "unknown"])
def test_completed_session_recovery_rejects_nonrecoverable_status(monkeypatch, status):
    monkeypatch.setattr(
        recovery,
        "finalize_normal_result",
        lambda **_: pytest.fail("invalid status must fail before scoring"),
    )

    session = _session(status=status)
    with pytest.raises(recovery.LegacyCompletedSessionStateInvalid):
        recovery.finalize_completed_session(
            user_id=42,
            session=session,
            username="u",
            first_name="User",
            achievement_rewards={},
        )


def test_completed_session_recovery_rejects_missing_status(monkeypatch):
    monkeypatch.setattr(
        recovery,
        "finalize_normal_result",
        lambda **_: pytest.fail("missing status must fail before scoring"),
    )

    session = _session()
    session.pop("status")
    with pytest.raises(recovery.LegacyCompletedSessionStateInvalid):
        recovery.finalize_completed_session(
            user_id=42,
            session=session,
            username="u",
            first_name="User",
            achievement_rewards={},
        )


def test_completed_session_recovery_rejects_missing_duration_evidence(monkeypatch):
    monkeypatch.setattr(
        recovery,
        "finalize_normal_result",
        lambda **_: pytest.fail("incomplete evidence must fail before scoring"),
    )
    session = _session(
        answered_questions=[
            {"index": 0, "qid": "q1", "is_correct": True, "ts": "2026-08-10T12:00:05"},
            {"index": 1, "qid": "q2", "is_correct": False},
        ]
    )

    with pytest.raises(recovery.LegacyCompletedSessionEvidenceIncomplete):
        recovery.finalize_completed_session(
            user_id=42,
            session=session,
            username="u",
            first_name="User",
            achievement_rewards={},
        )


def test_incomplete_session_is_never_scored_as_completed(monkeypatch):
    monkeypatch.setattr(
        recovery,
        "finalize_normal_result",
        lambda **_: pytest.fail("incomplete session must fail before scoring"),
    )

    with pytest.raises(recovery.LegacyCompletedSessionEvidenceIncomplete):
        recovery.finalize_completed_session(
            user_id=42,
            session=_session(current_index=1),
            username="u",
            first_name="User",
            achievement_rewards={},
        )
