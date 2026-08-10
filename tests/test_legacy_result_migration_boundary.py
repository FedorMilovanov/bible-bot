from pathlib import Path


BOT = (Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8")


def async_function(name: str) -> str:
    marker = f"async def {name}"
    start = BOT.index(marker)
    next_async = BOT.find("\nasync def ", start + len(marker))
    return BOT[start:] if next_async == -1 else BOT[start:next_async]


def test_mongo_authoritative_result_migration_is_all_or_nothing():
    migration_markers = (
        "from legacy_live_finalize import",
        "finalize_live_persisted_attempt(",
        "from legacy_attempt_finalize import",
        "finalize_normal_result(",
        "finalize_challenge_result(",
    )
    if not any(marker in BOT for marker in migration_markers):
        # The controller is intentionally still on the historical scoring path.
        # Once result migration starts, every invariant below becomes mandatory.
        return

    normal = async_function("show_results")
    challenge = async_function("show_challenge_results")

    assert "from legacy_live_finalize import" in BOT
    assert "from legacy_attempt_finalize import" not in BOT

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
