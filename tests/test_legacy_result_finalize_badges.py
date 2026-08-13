import legacy_result_finalize as finalize


def test_challenge_badges_persist_keys_and_return_legacy_messages(monkeypatch):
    data = {
        "session_id": "challenge-session",
        "level_key": "random20",
        "challenge_mode": "random20",
        "username": "u",
        "first_name": "User",
        "max_streak": 20,
        "start_time": 100.0,
        "questions": [{"id": "q1"}],
    }
    achievement_state = {
        "total_tests": 3,
        "perfect_count": 1,
        "max_streak_ever": 20,
        "daily_activity_streak": 1,
        "challenge_streak_count": 3,
    }
    durable_result = {
        "level_key": "random20",
        "score": 20,
        "total": 20,
        "time_seconds": 45,
        "score_multiplier": 1.0,
        "max_streak": 20,
        "challenge_mode": "random20",
        "quiz_mode": None,
        "fastest_answer": None,
        "earned_base": 20,
    }
    base = {
        "applied": True,
        "earned_base": 20,
        "completed_at": "2026-08-10T12:00:00",
        "receipt": {
            "completed_at": "2026-08-10T12:00:00",
            "daily_streak": 1,
            "challenge_streak": 3,
            "result": durable_result,
            "achievement_state": achievement_state,
        },
        "result": durable_result,
        "user": dict(achievement_state),
    }
    claimed_keys = []

    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "challenge-result")
    monkeypatch.setattr(finalize, "apply_base_result_once", lambda **_: base)
    monkeypatch.setattr(
        finalize,
        "claim_challenge_bonus_for_result",
        lambda **_: {"bonus": 100, "eligible": True, "claimed_now": True},
    )
    monkeypatch.setattr(finalize, "sync_weekly_best", lambda **_: None)
    monkeypatch.setattr(finalize, "general_achievement_candidates", lambda *_: [])

    def claim(_user_id, key, **_kwargs):
        claimed_keys.append(key)
        return True

    monkeypatch.setattr(finalize, "claim_achievement_once", claim)
    monkeypatch.setattr(
        finalize,
        "finish_completed_owned_quiz_session",
        lambda *_: {"status": "finished"},
    )

    result = finalize.finalize_challenge_result(
        user_id=42,
        data=data,
        score=20,
        total=20,
        time_seconds=45,
        achievement_rewards={},
    )

    assert claimed_keys == ["streak_3", "perfect_20"]
    assert result["new_challenge_badges"] == [
        "🔥 3-дневная серия 18+ — разблокировано!",
        "⭐ Perfect 20 — разблокировано!",
    ]
