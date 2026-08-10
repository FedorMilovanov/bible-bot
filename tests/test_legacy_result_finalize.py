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
    achievement_state = {
        "total_tests": 1,
        "perfect_count": 0,
        "max_streak_ever": 4,
        "daily_activity_streak": daily_streak,
        "challenge_streak_count": challenge_streak,
    }
    durable_result = {
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
    }
    return {
        "applied": True,
        "earned_base": 8,
        "completed_at": "2026-08-10T23:59:00",
        "receipt": {
            "completed_at": "2026-08-10T23:59:00",
            "daily_streak": daily_streak,
            "challenge_streak": challenge_streak,
            "result": durable_result,
            "achievement_state": achievement_state,
        },
        "result": durable_result,
        "user": dict(achievement_state),
    }


def test_normal_finalization_uses_durable_streak_and_finishes_last(monkeypatch):
    events = []
    captured = {}
    data = normal_data()

    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "result-1")
    monkeypatch.setattr(
        finalize,
        "apply_base_result_once",
        lambda **_: (events.append("base") or base_result()),
    )

    def daily(**kwargs):
        events.append("daily")
        captured.update(kwargs)
        return {"bonus": 10, "eligible": True, "claimed_now": True}

    monkeypatch.setattr(finalize, "claim_daily_bonus_for_result", daily)
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
    assert captured == {
        "user_id": 42,
        "result_id": "result-1",
        "day": "2026-08-10",
        "daily_streak": 3,
    }
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
        "claim_daily_bonus_for_result",
        lambda **_: {"bonus": 10, "eligible": True, "claimed_now": False},
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


def test_normal_retry_uses_result_time_achievement_policy_inputs(monkeypatch):
    data = normal_data()
    data.update({"quiz_mode": "relaxed", "fastest_answer": 99.0})
    base = base_result()
    base["applied"] = False
    base["receipt"]["result"]["quiz_mode"] = "speed"
    base["receipt"]["result"]["fastest_answer"] = 2.5
    base["receipt"]["achievement_state"] = {
        "total_tests": 10,
        "perfect_count": 5,
        "max_streak_ever": 10,
        "daily_activity_streak": 7,
        "challenge_streak_count": 0,
    }
    # Simulate a much later current user document. Policy must not consume it.
    base["user"] = {
        "total_tests": 100,
        "perfect_count": 15,
        "max_streak_ever": 20,
        "daily_activity_streak": 30,
    }
    captured = {}

    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "result-1")
    monkeypatch.setattr(finalize, "apply_base_result_once", lambda **_: base)
    monkeypatch.setattr(
        finalize,
        "claim_daily_bonus_for_result",
        lambda **_: {"bonus": 15, "eligible": True, "claimed_now": False},
    )

    def candidates(state, policy):
        captured["state"] = state
        captured["policy"] = policy
        return []

    monkeypatch.setattr(finalize, "general_achievement_candidates", candidates)
    monkeypatch.setattr(finalize, "finish_owned_quiz_session", lambda *_: {"status": "finished"})

    finalize.finalize_normal_result(
        user_id=42,
        data=data,
        score=1,
        total=10,
        time_seconds=999,
        achievement_rewards={},
    )

    assert captured["state"]["total_tests"] == 10
    assert captured["state"]["perfect_count"] == 5
    assert captured["policy"]["quiz_mode"] == "speed"
    assert captured["policy"]["fastest_answer"] == 2.5


def test_challenge_finalization_syncs_weekly_on_every_attempt(monkeypatch):
    events = []
    captured_weekly = {}
    data = challenge_data()
    base = base_result(challenge_streak=3)
    base["earned_base"] = 18
    base["result"] = dict(base["receipt"]["result"])
    base["receipt"]["result"].update({
        "level_key": "random20",
        "score": 18,
        "total": 20,
        "time_seconds": 50,
        "challenge_mode": "random20",
        "earned_base": 18,
    })
    base["result"] = dict(base["receipt"]["result"])
    base["receipt"]["achievement_state"]["challenge_streak_count"] = 3
    base["user"]["challenge_streak_count"] = 3

    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "challenge-1")
    monkeypatch.setattr(
        finalize,
        "apply_base_result_once",
        lambda **_: (events.append("base") or base),
    )
    monkeypatch.setattr(
        finalize,
        "claim_challenge_bonus_for_result",
        lambda **_: (events.append("bonus") or {"bonus": 60, "eligible": True, "claimed_now": False}),
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


def test_challenge_retry_uses_durable_score_and_time(monkeypatch):
    data = challenge_data()
    base = base_result(challenge_streak=3)
    base["applied"] = False
    base["receipt"]["result"].update({
        "level_key": "random20",
        "score": 18,
        "total": 20,
        "time_seconds": 44.5,
        "challenge_mode": "random20",
        "earned_base": 18,
    })
    base["receipt"]["achievement_state"]["challenge_streak_count"] = 3
    captured = {}

    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "challenge-1")
    monkeypatch.setattr(finalize, "apply_base_result_once", lambda **_: base)

    def bonus(**kwargs):
        captured["bonus"] = kwargs
        return {"bonus": 60, "eligible": True, "claimed_now": False}

    def weekly(**kwargs):
        captured["weekly"] = kwargs

    def badges(state, durable_score):
        captured["badge_score"] = durable_score
        captured["badge_state"] = state
        return []

    monkeypatch.setattr(finalize, "claim_challenge_bonus_for_result", bonus)
    monkeypatch.setattr(finalize, "sync_weekly_best", weekly)
    monkeypatch.setattr(finalize, "general_achievement_candidates", lambda *_: [])
    monkeypatch.setattr(finalize, "challenge_badge_candidates", badges)
    monkeypatch.setattr(finalize, "finish_owned_quiz_session", lambda *_: {"status": "finished"})

    finalize.finalize_challenge_result(
        user_id=42,
        data=data,
        score=1,
        total=20,
        time_seconds=999,
        achievement_rewards={},
    )

    assert captured["bonus"]["score"] == 18
    assert captured["bonus"]["result_id"] == "challenge-1"
    assert captured["weekly"]["score"] == 18
    assert captured["weekly"]["time_seconds"] == 44.5
    assert captured["badge_score"] == 18
    assert captured["badge_state"]["challenge_streak_count"] == 3


def test_session_finish_failure_is_retryable_after_durable_scoring(monkeypatch):
    data = normal_data()
    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "result-1")
    monkeypatch.setattr(finalize, "apply_base_result_once", lambda **_: base_result())
    monkeypatch.setattr(
        finalize,
        "claim_daily_bonus_for_result",
        lambda **_: {"bonus": 10, "eligible": True, "claimed_now": False},
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
