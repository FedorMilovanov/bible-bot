from datetime import datetime

import pytest

import legacy_session_finalize as recovery


def _session(**overrides):
    session = {
        "_id": "session-1",
        "user_id": "42",
        "mode": "level",
        "questions_data": [{"id": "q1"}, {"id": "q2"}],
        "current_index": 2,
        "correct_count": 1,
        "answered_questions": [
            {"is_correct": True, "ts": "2026-08-10T12:00:05"},
            {"is_correct": False, "ts": "2026-08-10T12:00:20"},
        ],
        "level_key": "easy",
        "level_name": "Easy",
        "chat_id": 42,
        "start_time": datetime(2026, 8, 10, 12, 0, 0).timestamp(),
        "time_limit": None,
    }
    session.update(overrides)
    return session


def test_completed_normal_session_uses_persisted_result_inputs(monkeypatch):
    captured = {}

    def normal(**kwargs):
        captured.update(kwargs)
        return {"scored": True, "result_id": "quiz:session-1"}

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
    assert captured["data"]["username"] == "tester"
    assert captured["data"]["first_name"] == "Test"
    assert captured["data"]["result_pending"] is True
    assert captured["data"]["result_completed_at"] == "2026-08-10T12:00:20"


def test_completed_challenge_session_routes_to_challenge_finalizer(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        recovery,
        "finalize_normal_result",
        lambda **_: pytest.fail("Challenge session must not use normal finalizer"),
    )

    def challenge(**kwargs):
        captured.update(kwargs)
        return {"scored": True, "result_id": "quiz:session-1"}

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


def test_completed_session_recovery_rejects_missing_duration_evidence(monkeypatch):
    monkeypatch.setattr(
        recovery,
        "finalize_normal_result",
        lambda **_: pytest.fail("incomplete evidence must fail before scoring"),
    )
    session = _session(
        answered_questions=[
            {"is_correct": True, "ts": "2026-08-10T12:00:05"},
            {"is_correct": False},
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
