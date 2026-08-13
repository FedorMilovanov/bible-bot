from pathlib import Path


CONTROLLER = (
    Path(__file__).resolve().parents[1] / "telegram_controller.py"
).read_text(encoding="utf-8")


def async_function(name: str) -> str:
    marker = f"async def {name}"
    start = CONTROLLER.index(marker)
    next_async = CONTROLLER.find("\nasync def ", start + len(marker))
    return CONTROLLER[start:] if next_async == -1 else CONTROLLER[start:next_async]


def test_mongo_authoritative_result_is_the_only_production_path():
    for marker in (
        "launch_quiz_attempt(",
        "restart_owned_quiz_attempt(",
        "cancel_current_incomplete_session(",
        "resolve_session_action(",
        "session_action_payloads(",
    ):
        assert marker in CONTROLLER

    for destructive in (
        "create_quiz_session(",
        "cancel_active_quiz_session(",
        "cancel_quiz_session(",
    ):
        assert destructive not in CONTROLLER

    normal = async_function("show_results")
    challenge = async_function("show_challenge_results")

    assert "from legacy_live_finalize import" in CONTROLLER
    assert "from legacy_attempt_finalize import" not in CONTROLLER

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
