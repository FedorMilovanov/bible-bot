import pytest

import legacy_result_finalize as finalize
from legacy_result_store import LegacyResultStoreUnavailable
from legacy_session_close import QuizSessionCompletionInvalid


def _normal_data():
    return {
        "session_id": "s1",
        "level_key": "easy",
        "username": "u",
        "first_name": "User",
        "score_multiplier": 1.0,
        "max_streak": 1,
    }


def _session(*, mode="level", score=1, total=2, level_key="easy"):
    return {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "mode": mode,
        "level_key": level_key,
        "correct_count": score,
        "question_ids": [f"q{i}" for i in range(total)],
    }


def test_normal_preflight_requires_matching_durable_score_total_and_level(monkeypatch):
    monkeypatch.setattr(
        finalize,
        "validate_completed_owned_quiz_session",
        lambda *_: _session(),
    )

    proven = finalize._preflight_recovery_session(
        user_id=42,
        data=_normal_data(),
        score=1,
        total=2,
        challenge_mode=None,
    )
    assert proven["mode"] == "level"

    with pytest.raises(LegacyResultStoreUnavailable, match="does not match"):
        finalize._preflight_recovery_session(
            user_id=42,
            data=_normal_data(),
            score=0,
            total=2,
            challenge_mode=None,
        )

    with pytest.raises(LegacyResultStoreUnavailable, match="does not match"):
        finalize._preflight_recovery_session(
            user_id=42,
            data=_normal_data(),
            score=1,
            total=3,
            challenge_mode=None,
        )

    monkeypatch.setattr(
        finalize,
        "validate_completed_owned_quiz_session",
        lambda *_: _session(level_key="hard"),
    )
    with pytest.raises(LegacyResultStoreUnavailable, match="level does not match"):
        finalize._preflight_recovery_session(
            user_id=42,
            data=_normal_data(),
            score=1,
            total=2,
            challenge_mode=None,
        )


def test_preflight_rejects_missing_session_and_mode_family_mismatch(monkeypatch):
    data = _normal_data()
    data.pop("session_id")
    with pytest.raises(LegacyResultStoreUnavailable, match="session id is missing"):
        finalize._preflight_recovery_session(
            user_id=42,
            data=data,
            score=1,
            total=2,
            challenge_mode=None,
        )

    monkeypatch.setattr(
        finalize,
        "validate_completed_owned_quiz_session",
        lambda *_: _session(mode="random20", level_key="random20"),
    )
    with pytest.raises(LegacyResultStoreUnavailable, match="normal durable session"):
        finalize._preflight_recovery_session(
            user_id=42,
            data=_normal_data(),
            score=1,
            total=2,
            challenge_mode=None,
        )

    with pytest.raises(LegacyResultStoreUnavailable, match="Challenge result mode"):
        finalize._preflight_recovery_session(
            user_id=42,
            data={**_normal_data(), "challenge_mode": "hardcore20"},
            score=1,
            total=2,
            challenge_mode="hardcore20",
        )


def test_normal_finalizer_never_writes_base_result_before_completion_proof(monkeypatch):
    monkeypatch.setattr(
        finalize,
        "validate_completed_owned_quiz_session",
        lambda *_: (_ for _ in ()).throw(
            QuizSessionCompletionInvalid("incomplete")
        ),
    )
    monkeypatch.setattr(
        finalize,
        "apply_base_result_once",
        lambda **_: pytest.fail("base scoring must not run before completion proof"),
    )

    with pytest.raises(finalize.LegacyResultFinalizationPending):
        finalize.finalize_normal_result(
            user_id=42,
            data=_normal_data(),
            score=1,
            total=2,
            time_seconds=5,
            achievement_rewards={},
        )


def test_challenge_finalizer_never_writes_base_result_before_completion_proof(monkeypatch):
    monkeypatch.setattr(
        finalize,
        "validate_completed_owned_quiz_session",
        lambda *_: (_ for _ in ()).throw(
            QuizSessionCompletionInvalid("incomplete")
        ),
    )
    monkeypatch.setattr(
        finalize,
        "apply_base_result_once",
        lambda **_: pytest.fail("Challenge base scoring must wait for completion proof"),
    )

    with pytest.raises(finalize.LegacyResultFinalizationPending):
        finalize.finalize_challenge_result(
            user_id=42,
            data={**_normal_data(), "challenge_mode": "random20"},
            score=1,
            total=2,
            time_seconds=5,
            achievement_rewards={},
        )
