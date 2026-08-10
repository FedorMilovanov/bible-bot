import pytest

import legacy_result_finalize as finalize
from legacy_result_store import LegacyResultStoreUnavailable
from session_integrity import QuizSessionStoreUnavailable


def normal_data():
    return {
        "session_id": "s1",
        "level_key": "easy",
        "username": "u",
        "first_name": "User",
        "score_multiplier": 1.0,
        "max_streak": 4,
        "start_time": 100.0,
        "questions": [{"id": "q1"}],
        "answered_questions": [],
    }


def challenge_data():
    data = normal_data()
    data.update({
        "level_key": "random20",
        "challenge_mode": "random20",
        "is_challenge": True,
        "max_streak": 7,
    })
    return data


def base_result(*, daily_streak=3, challenge_streak=0):
    return {
        "applied": True,
        "earned_base": 8,
        "completed_at": "2026-08-10T23:59:00",
        "receipt": {
            "completed_at": "2026-08-10T23:59:00",
            "daily_streak": daily_streak,
            "challenge_streak": challenge_streak,
        },
        "user": {
            "total_tests": 1,
            "perfect_count": 0,
            "max_streak_ever": 4,
            "daily_activity_streak": daily_streak,
            "challenge_streak_count": challenge_streak,
            "achievements": {},
        },
    }


def test_normal_finalization_uses_durable_streak_and_finishes_last(monkeypatch):
    events = []
    captured = {}
    data = normal_data()

    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "result-1")
    monkeypatch.setattr(finalize, "apply_base_result_once", lambda **_: (events.append("base") or base_result()))

    def daily(user_id, day, daily_streak):
        events.append("daily")
        captured.update({"user_id": user_id, "day": day, "daily_streak": daily_streak})
        return {"bonus": 10, "eligible": True, "claimed_now": True}

    monkeypatch.setattr(finalize, "claim_daily_bonus_state", daily)
    monkeypatch.setattr(finalize, "general_achievement_candidates", lambda *_: ["first_steps"])
    monkeypatch.setattr(
        finalize,
        "claim_achievement_once",
        lambda *_, **__: (events.append("achievement") or True),
    )
    monkeypatch.setattr(
        finalize,
        "finish_owned_quiz_session",
        lambda *_: (events.append("finish") or {"status": "finished"}),
    )

    result = finalize.finalize_normal_result(
        user_id=42,
        data=data,
        score=8,
        total=10,
        time_seconds=12,
        achievement_rewards={"first_steps": 10},
    )

    assert result["earned_base"] == 8
    assert result["daily_bonus"]["bonus"] == 10
    assert result["new_achievements"] == ["first_steps"]
    assert captured == {"user_id": 42, "day": "2026-08-10", "daily_streak": 3}
    assert events == ["base", "daily", "achievement", "finish"]


def test_retry_error_drill_never_enters_scoring(monkeypatch):
    data = normal_data()
    data["is_retry"] = True
    monkeypatch.setattr(
        finalize,
        "apply_base_result_once",
        lambda **_: pytest.fail("retry-error drill must not enter scoring"),
    )

    result = finalize.finalize_normal_result(
        user_id=42,
        data=data,
        score=2,
        total=3,
        time_seconds=5,
        achievement_rewards={},
    )

    assert result["scored"] is False
    assert result["earned_base"] == 0


def test_partial_achievement_failure_keeps_result_retryable(monkeypatch):
    data = normal_data()
    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "result-1")
    monkeypatch.setattr(finalize, "apply_base_result_once", lambda **_: base_result())
    monkeypatch.setattr(
        finalize,
        "claim_daily_bonus_state",
        lambda *_args, **_kwargs: {"bonus": 10, "eligible": True, "claimed_now": False},
    )
    monkeypatch.setattr(finalize, "general_achievement_candidates", lambda *_: ["first_steps"])
    monkeypatch.setattr(
        finalize,
        "claim_achievement_once",
        lambda *_, **__: (_ for _ in ()).throw(LegacyResultStoreUnavailable("mongo")),
    )

    with pytest.raises(finalize.LegacyResultFinalizationPending):
        finalize.finalize_normal_result(
            user_id=42,
            data=data,
            score=8,
            total=10,
            time_seconds=12,
            achievement_rewards={"first_steps": 10},
        )


def test_challenge_finalization_syncs_weekly_on_every_attempt(monkeypatch):
    events = []
    captured_weekly = {}
    data = challenge_data()
    base = base_result(challenge_streak=3)
    base["earned_base"] = 18
    base["user"]["challenge_streak_count"] = 3

    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "challenge-1")
    monkeypatch.setattr(finalize, "apply_base_result_once", lambda **_: (events.append("base") or base))
    monkeypatch.setattr(
        finalize,
        "claim_challenge_bonus_state",
        lambda *_: (events.append("bonus") or {"bonus": 60, "eligible": True, "claimed_now": False}),
    )

    def weekly(**kwargs):
        events.append("weekly")
        captured_weekly.update(kwargs)

    monkeypatch.setattr(finalize, "sync_weekly_best", weekly)
    monkeypatch.setattr(finalize, "general_achievement_candidates", lambda *_: [])
    monkeypatch.setattr(
        finalize,
        "challenge_badge_candidates",
        lambda *_: [("streak_3", "🔥 3-дневная серия 18+ — разблокировано!")],
    )
    monkeypatch.setattr(
        finalize,
        "claim_achievement_once",
        lambda *_, **__: (events.append("badge") or True),
    )
    monkeypatch.setattr(
        finalize,
        "finish_owned_quiz_session",
        lambda *_: (events.append("finish") or {"status": "finished"}),
    )

    result = finalize.finalize_challenge_result(
        user_id=42,
        data=data,
        score=18,
        total=20,
        time_seconds=50,
        achievement_rewards={},
    )

    assert result["bonus"] == {"bonus": 60, "eligible": True, "claimed_now": False}
    assert result["new_challenge_badges"] == [
        "🔥 3-дневная серия 18+ — разблокировано!"
    ]
    assert captured_weekly["week_id"] == "2026-W33"
    assert captured_weekly["score"] == 18
    assert events == ["base", "bonus", "weekly", "badge", "finish"]


def test_session_finish_failure_is_retryable_after_durable_scoring(monkeypatch):
    data = normal_data()
    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "result-1")
    monkeypatch.setattr(finalize, "apply_base_result_once", lambda **_: base_result())
    monkeypatch.setattr(
        finalize,
        "claim_daily_bonus_state",
        lambda *_args, **_kwargs: {"bonus": 10, "eligible": True, "claimed_now": False},
    )
    monkeypatch.setattr(finalize, "general_achievement_candidates", lambda *_: [])
    monkeypatch.setattr(
        finalize,
        "finish_owned_quiz_session",
        lambda *_: (_ for _ in ()).throw(QuizSessionStoreUnavailable("mongo")),
    )

    with pytest.raises(finalize.LegacyResultFinalizationPending):
        finalize.finalize_normal_result(
            user_id=42,
            data=data,
            score=8,
            total=10,
            time_seconds=12,
            achievement_rewards={},
        )
