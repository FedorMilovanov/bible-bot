from __future__ import annotations

from pathlib import Path


RUNTIME = (
    Path(__file__).resolve().parents[1] / "telegram_quiz_runtime_controller.py"
).read_text(encoding="utf-8")


def async_function(name: str) -> str:
    marker = f"async def {name}"
    start = RUNTIME.index(marker)
    next_async = RUNTIME.find("\nasync def ", start + len(marker))
    return RUNTIME[start:] if next_async == -1 else RUNTIME[start:next_async]


def test_mongo_authoritative_result_is_the_only_quiz_runtime_path():
    for marker in (
        "launch_quiz_attempt(",
        "cancel_current_incomplete_session(",
        "resolve_session_action(",
        "session_action_payloads(",
    ):
        assert marker in RUNTIME

    for destructive in (
        "create_quiz_session(",
        "cancel_active_quiz_session(",
        "cancel_quiz_session(",
    ):
        assert destructive not in RUNTIME

    normal = async_function("show_results")
    challenge = async_function("show_challenge_results")

    assert "from legacy_live_finalize import" in RUNTIME
    assert "from legacy_attempt_finalize import" not in RUNTIME

    for source in (normal, challenge):
        assert "finalize_live_persisted_attempt(" in source
        assert "finish_quiz_session(" not in source
        assert "collection.update_one(" not in source

    for marker in (
        "add_to_leaderboard(",
        "update_daily_streak(",
        "update_achievement_stats(",
        "check_daily_bonus(",
    ):
        assert marker not in normal

    for marker in (
        "update_challenge_stats(",
        "update_weekly_leaderboard(",
        "compute_bonus(",
    ):
        assert marker not in challenge
