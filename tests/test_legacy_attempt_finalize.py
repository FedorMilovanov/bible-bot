import pytest

import legacy_attempt_finalize as attempt_finalize


def _data(*, attempt_id="attempt-1"):
    return {
        "session_id": "container-1",
        "attempt_id": attempt_id,
        "level_key": "easy",
    }


def _session(*, attempt_id="attempt-1"):
    return {
        "_id": "container-1",
        "attempt_id": attempt_id,
        "user_id": "42",
        "status": "in_progress",
        "current_index": 1,
        "correct_count": 1,
        "question_ids": ["q1"],
        "answered_questions": [
            {"index": 0, "qid": "q1", "is_correct": True}
        ],
    }


def test_normal_attempt_proof_runs_before_underlying_finalizer(monkeypatch):
    events = []
    monkeypatch.setattr(
        attempt_finalize,
        "validate_completed_owned_quiz_session",
        lambda session_id, user_id: events.append(("proof", session_id, user_id)) or _session(),
    )
    monkeypatch.setattr(
        attempt_finalize,
        "_finalize_normal_result",
        lambda **kwargs: events.append(("finalize", kwargs["score"])) or {"scored": True},
    )

    result = attempt_finalize.finalize_normal_result(
        user_id=42,
        data=_data(),
        score=1,
        total=1,
        time_seconds=2.0,
        achievement_rewards={},
    )

    assert result == {"scored": True}
    assert events == [("proof", "container-1", 42), ("finalize", 1)]


def test_stale_runtime_attempt_never_enters_scoring(monkeypatch):
    monkeypatch.setattr(
        attempt_finalize,
        "validate_completed_owned_quiz_session",
        lambda *_: _session(attempt_id="attempt-new"),
    )
    monkeypatch.setattr(
        attempt_finalize,
        "_finalize_normal_result",
        lambda **_: pytest.fail("stale attempt must not enter result scoring"),
    )

    with pytest.raises(
        attempt_finalize.LegacyAttemptFinalizationPending,
        match="different durable quiz attempt",
    ):
        attempt_finalize.finalize_normal_result(
            user_id=42,
            data=_data(attempt_id="attempt-old"),
            score=1,
            total=1,
            time_seconds=2.0,
            achievement_rewards={},
        )


def test_same_container_restart_changes_result_identity_and_blocks_old_result(monkeypatch):
    durable = _session(attempt_id="attempt-new")
    monkeypatch.setattr(
        attempt_finalize,
        "validate_completed_owned_quiz_session",
        lambda *_: durable,
    )

    with pytest.raises(attempt_finalize.LegacyAttemptFinalizationPending):
        attempt_finalize._prove_current_attempt(42, _data(attempt_id="attempt-old"))
    assert attempt_finalize._prove_current_attempt(42, _data(attempt_id="attempt-new")) == "attempt-new"


def test_legacy_session_without_attempt_id_remains_compatible(monkeypatch):
    durable = _session()
    durable.pop("attempt_id")
    monkeypatch.setattr(
        attempt_finalize,
        "validate_completed_owned_quiz_session",
        lambda *_: durable,
    )

    data = {"session_id": "container-1", "level_key": "easy"}
    assert attempt_finalize._prove_current_attempt(42, data) == "container-1"


def test_challenge_uses_same_attempt_proof(monkeypatch):
    called = []
    monkeypatch.setattr(
        attempt_finalize,
        "validate_completed_owned_quiz_session",
        lambda *_: _session(),
    )
    monkeypatch.setattr(
        attempt_finalize,
        "_finalize_challenge_result",
        lambda **kwargs: called.append(kwargs) or {"scored": True},
    )

    result = attempt_finalize.finalize_challenge_result(
        user_id=42,
        data={"session_id": "container-1", "attempt_id": "attempt-1", "challenge_mode": "random20"},
        score=18,
        total=20,
        time_seconds=30.0,
        achievement_rewards={},
    )

    assert result["scored"] is True
    assert called[0]["score"] == 18


def test_retry_error_drill_bypasses_durable_attempt_proof(monkeypatch):
    monkeypatch.setattr(
        attempt_finalize,
        "validate_completed_owned_quiz_session",
        lambda *_: pytest.fail("memory-only retry drill must not require durable session"),
    )
    monkeypatch.setattr(
        attempt_finalize,
        "_finalize_normal_result",
        lambda **_: {"scored": False},
    )

    assert attempt_finalize.finalize_normal_result(
        user_id=42,
        data={"is_retry": True, "session_id": None},
        score=0,
        total=1,
        time_seconds=1.0,
        achievement_rewards={},
    ) == {"scored": False}
