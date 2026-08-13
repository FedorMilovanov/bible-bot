import pytest

import legacy_result_finalize as finalize
import legacy_result_store as store


def _base(completed_at):
    return {
        "applied": False,
        "earned_base": 8,
        "completed_at": completed_at,
        "receipt": {
            "completed_at": completed_at,
            "daily_streak": 3,
            "challenge_streak": 0,
            "result": {
                "level_key": "easy",
                "score": 8,
                "total": 10,
                "time_seconds": 12,
                "score_multiplier": 1.0,
                "max_streak": 4,
                "challenge_mode": None,
                "quiz_mode": None,
                "fastest_answer": None,
                "earned_base": 8,
            },
            "achievement_state": {
                "total_tests": 1,
                "perfect_count": 0,
                "max_streak_ever": 4,
                "daily_activity_streak": 3,
                "challenge_streak_count": 0,
            },
        },
        "user": {},
    }


def _normal_data():
    return {
        "session_id": "s1",
        "level_key": "easy",
        "username": "u",
        "first_name": "User",
        "score_multiplier": 1.0,
        "max_streak": 4,
        "questions": [{"id": "q1"}],
    }


def test_timestamp_validator_rejects_missing_and_malformed_values():
    with pytest.raises(store.LegacyResultStoreUnavailable, match="timestamp is missing"):
        finalize._validated_completed_at({})
    with pytest.raises(store.LegacyResultStoreUnavailable, match="timestamp is invalid"):
        finalize._validated_completed_at({"completed_at": "not-a-date"})


def test_malformed_normal_receipt_stays_retryable_before_bonus_or_finish(monkeypatch):
    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "result-1")
    monkeypatch.setattr(finalize, "apply_base_result_once", lambda **_: _base("not-a-date"))
    monkeypatch.setattr(
        finalize,
        "claim_daily_bonus_for_result",
        lambda **_: pytest.fail("invalid timestamp must stop before bonus"),
    )
    monkeypatch.setattr(
        finalize,
        "finish_completed_owned_quiz_session",
        lambda *_: pytest.fail("invalid timestamp must stop before session finish"),
    )

    with pytest.raises(finalize.LegacyResultFinalizationPending):
        finalize.finalize_normal_result(
            user_id=42,
            data=_normal_data(),
            score=8,
            total=10,
            time_seconds=12,
            achievement_rewards={},
        )


def test_malformed_challenge_receipt_stays_retryable_before_bonus_or_weekly(monkeypatch):
    data = _normal_data()
    data.update({"level_key": "random20", "challenge_mode": "random20"})
    base = _base("broken")
    base["receipt"]["result"].update({
        "level_key": "random20",
        "score": 18,
        "total": 20,
        "challenge_mode": "random20",
    })

    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "challenge-1")
    monkeypatch.setattr(finalize, "apply_base_result_once", lambda **_: base)
    monkeypatch.setattr(
        finalize,
        "claim_challenge_bonus_for_result",
        lambda **_: pytest.fail("invalid timestamp must stop before Challenge bonus"),
    )
    monkeypatch.setattr(
        finalize,
        "sync_weekly_best",
        lambda **_: pytest.fail("invalid timestamp must stop before weekly sync"),
    )

    with pytest.raises(finalize.LegacyResultFinalizationPending):
        finalize.finalize_challenge_result(
            user_id=42,
            data=data,
            score=18,
            total=20,
            time_seconds=50,
            achievement_rewards={},
        )
