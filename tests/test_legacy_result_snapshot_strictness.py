import pytest

import legacy_result_finalize as finalize


def _normal_data():
    return {
        "session_id": "s1",
        "level_key": "easy",
        "username": "u",
        "first_name": "User",
        "score_multiplier": 1.0,
        "max_streak": 2,
        "questions": [{"id": "q1"}],
    }


def _challenge_data():
    data = _normal_data()
    data.update({
        "level_key": "random20",
        "challenge_mode": "random20",
        "is_challenge": True,
    })
    return data


def _base(receipt):
    return {
        "applied": False,
        "earned_base": 1,
        "completed_at": "2026-08-10T12:00:00",
        "receipt": receipt,
        "user": {
            "total_tests": 999,
            "perfect_count": 999,
            "max_streak_ever": 999,
            "daily_activity_streak": 999,
            "challenge_streak_count": 999,
        },
    }


def _full_result():
    return {
        "level_key": "easy",
        "score": 1,
        "total": 1,
        "time_seconds": 2.0,
        "score_multiplier": 1.0,
        "max_streak": 1,
        "challenge_mode": None,
        "quiz_mode": None,
        "fastest_answer": None,
        "earned_base": 1,
    }


def _challenge_result():
    result = _full_result()
    result.update({
        "level_key": "random20",
        "score": 18,
        "total": 20,
        "time_seconds": 40.0,
        "max_streak": 7,
        "challenge_mode": "random20",
        "earned_base": 18,
    })
    return result


def _full_achievement_state():
    return {
        "total_tests": 1,
        "perfect_count": 1,
        "max_streak_ever": 1,
        "daily_activity_streak": 1,
        "challenge_streak_count": 0,
    }


def _receipt(result, achievement_state=None):
    return {
        "completed_at": "2026-08-10T12:00:00",
        "daily_streak": 1,
        "challenge_streak": 0,
        "result": result,
        "achievement_state": achievement_state or _full_achievement_state(),
    }


def _assert_normal_pending_before_bonus(monkeypatch, base):
    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "result-1")
    monkeypatch.setattr(finalize, "apply_base_result_once", lambda **_: base)
    monkeypatch.setattr(
        finalize,
        "claim_daily_bonus_for_result",
        lambda **_: pytest.fail("invalid durable snapshot must stop before bonus"),
    )
    monkeypatch.setattr(
        finalize,
        "finish_completed_owned_quiz_session",
        lambda *_: pytest.fail("invalid durable snapshot must stop before finish"),
    )

    with pytest.raises(finalize.LegacyResultFinalizationPending):
        finalize.finalize_normal_result(
            user_id=42,
            data=_normal_data(),
            score=1,
            total=1,
            time_seconds=2.0,
            achievement_rewards={},
        )


def _assert_challenge_pending_before_bonus(monkeypatch, base):
    monkeypatch.setattr(finalize, "stable_result_id", lambda *_: "challenge-1")
    monkeypatch.setattr(finalize, "apply_base_result_once", lambda **_: base)
    monkeypatch.setattr(
        finalize,
        "claim_challenge_bonus_for_result",
        lambda **_: pytest.fail("invalid Challenge snapshot must stop before bonus"),
    )
    monkeypatch.setattr(
        finalize,
        "sync_weekly_best",
        lambda **_: pytest.fail("invalid Challenge snapshot must stop before weekly sync"),
    )
    monkeypatch.setattr(
        finalize,
        "finish_completed_owned_quiz_session",
        lambda *_: pytest.fail("invalid Challenge snapshot must stop before finish"),
    )

    with pytest.raises(finalize.LegacyResultFinalizationPending):
        finalize.finalize_challenge_result(
            user_id=42,
            data=_challenge_data(),
            score=18,
            total=20,
            time_seconds=40.0,
            achievement_rewards={},
        )


def test_missing_durable_result_snapshot_does_not_fallback_to_handler_args(monkeypatch):
    _assert_normal_pending_before_bonus(
        monkeypatch,
        _base({
            "completed_at": "2026-08-10T12:00:00",
            "daily_streak": 1,
            "challenge_streak": 0,
            "achievement_state": _full_achievement_state(),
        }),
    )


def test_incomplete_durable_result_snapshot_is_retryable(monkeypatch):
    result = _full_result()
    result.pop("time_seconds")
    _assert_normal_pending_before_bonus(monkeypatch, _base(_receipt(result)))


def test_missing_achievement_snapshot_does_not_fallback_to_current_user_doc(monkeypatch):
    receipt = _receipt(_full_result())
    receipt.pop("achievement_state")
    _assert_normal_pending_before_bonus(monkeypatch, _base(receipt))


def test_incomplete_achievement_snapshot_is_retryable(monkeypatch):
    achievement_state = _full_achievement_state()
    achievement_state.pop("total_tests")
    _assert_normal_pending_before_bonus(
        monkeypatch,
        _base(_receipt(_full_result(), achievement_state)),
    )


def test_string_score_is_not_accepted_as_durable_numeric_evidence(monkeypatch):
    result = _full_result()
    result["score"] = "1"
    _assert_normal_pending_before_bonus(monkeypatch, _base(_receipt(result)))


def test_unknown_quiz_mode_is_retryable_instead_of_inventing_policy(monkeypatch):
    result = _full_result()
    result["quiz_mode"] = "turbo"
    _assert_normal_pending_before_bonus(monkeypatch, _base(_receipt(result)))


def test_normal_result_refuses_durable_challenge_mode(monkeypatch):
    result = _full_result()
    result["challenge_mode"] = "random20"
    _assert_normal_pending_before_bonus(monkeypatch, _base(_receipt(result)))


def test_challenge_result_requires_durable_challenge_mode(monkeypatch):
    result = _challenge_result()
    result["challenge_mode"] = None
    _assert_challenge_pending_before_bonus(monkeypatch, _base(_receipt(result)))
