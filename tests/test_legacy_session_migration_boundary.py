from pathlib import Path


BOT = (Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8")


def async_function(name: str) -> str:
    marker = f"async def {name}"
    start = BOT.index(marker)
    next_async = BOT.find("\nasync def ", start + len(marker))
    return BOT[start:] if next_async == -1 else BOT[start:next_async]


def test_strict_session_lifecycle_migration_is_all_or_nothing():
    migration_markers = (
        "from legacy_session_launch import",
        "launch_quiz_attempt(",
        "from legacy_session_lifecycle import",
        "restart_owned_quiz_attempt(",
        "from legacy_session_control import",
        "cancel_current_incomplete_session(",
    )
    if not any(marker in BOT for marker in migration_markers):
        # The controller is intentionally still on the historical lifecycle.
        # Once strict lifecycle migration starts, destructive APIs are forbidden.
        return

    assert "launch_quiz_attempt(" in BOT
    assert "restart_owned_quiz_attempt(" in BOT
    assert "cancel_current_incomplete_session(" in BOT

    # No controller path may recreate the historical cancel -> insert window or
    # erase exact-completed result evidence through global active cancellation.
    assert "create_quiz_session(" not in BOT
    assert "cancel_active_quiz_session(" not in BOT
    assert "cancel_quiz_session(" not in BOT

    restart = async_function("restart_session_handler")
    reset_command = async_function("reset_command")
    reset_inline = async_function("reset_session_inline")

    assert "restart_owned_quiz_attempt(" in restart
    assert "cancel_current_incomplete_session(" in reset_command
    assert "cancel_current_incomplete_session(" in reset_inline
