from pathlib import Path


BOT = (Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8")


def async_function(name: str) -> str:
    marker = f"async def {name}("
    start = BOT.index(marker)
    next_async = BOT.find("\nasync def ", start + len(marker))
    return BOT[start:] if next_async == -1 else BOT[start:next_async]


def _assert_before(source: str, first: str, later: str) -> None:
    assert first in source
    assert later in source
    assert source.index(first) < source.index(later)


def test_strict_session_lifecycle_migration_is_all_or_nothing():
    migration_markers = (
        "from legacy_session_launch import",
        "launch_quiz_attempt(",
        "from legacy_session_lifecycle import",
        "restart_owned_quiz_attempt(",
        "from legacy_session_control import",
        "cancel_current_incomplete_session(",
        "from legacy_session_action import",
        "resolve_session_action(",
        "session_action_payloads(",
    )
    if not any(marker in BOT for marker in migration_markers):
        # The controller is intentionally still on the historical lifecycle.
        # Once strict lifecycle migration starts, destructive APIs are forbidden.
        return

    assert "launch_quiz_attempt(" in BOT
    assert "restart_owned_quiz_attempt(" in BOT
    assert "cancel_current_incomplete_session(" in BOT
    assert "resolve_session_action(" in BOT
    assert "session_action_payloads(" in BOT

    # No controller path may recreate the historical cancel -> insert window or
    # erase exact-completed result evidence through global active cancellation.
    assert "create_quiz_session(" not in BOT
    assert "cancel_active_quiz_session(" not in BOT
    assert "cancel_quiz_session(" not in BOT

    start = async_function("start")
    resume = async_function("resume_session_handler")
    restart = async_function("restart_session_handler")
    cancel_session = async_function("cancel_session_handler")
    cancel_quiz = async_function("cancel_quiz_handler")
    cancel_command = async_function("cancel")
    reset_command = async_function("reset_command")
    reset_inline = async_function("reset_session_inline")

    # `/start` is a status/recovery entry point, not a cancellation action. It
    # must distinguish incomplete resume evidence from exact-completed evidence
    # instead of merely switching to an owner-scoped destructive helper.
    assert "get_active_quiz_session_strict(" in start
    assert "classify_restart_session(" in start
    for destructive in (
        "cancel_owned_quiz_session(",
        "cancel_current_incomplete_session(",
        "restart_owned_quiz_attempt(",
    ):
        assert destructive not in start

    # In-place restart preserves the Mongo container id. Therefore session-id-
    # only legacy buttons become unsafe: an old pre-restart button could target
    # the replacement attempt. All three lifecycle buttons must resolve the
    # attempt token against current durable state before any resume/mutation.
    for source, action, legacy_prefix in (
        (resume, "res", "resume_session_"),
        (restart, "rst", "restart_session_"),
        (cancel_session, "can", "cancel_session_"),
    ):
        assert "resolve_session_action(" in source
        assert f'"{action}"' in source
        assert f'.replace("{legacy_prefix}", "")' not in source

    _assert_before(restart, "resolve_session_action(", "restart_owned_quiz_attempt(")
    assert "cancel_owned_quiz_session(" not in restart
    assert "expected_attempt_id=" in restart

    _assert_before(
        cancel_session,
        "resolve_session_action(",
        "cancel_owned_incomplete_quiz_attempt(",
    )
    assert "cancel_owned_quiz_session(" not in cancel_session
    assert "expected_attempt_id=" in cancel_session

    # Command/global exit paths resolve whichever attempt is current and then
    # use the completion-safe attempt-bound lifecycle CAS. They must not merely
    # clear RAM and leave Mongo active, nor call the historical global cancel.
    for source in (cancel_quiz, cancel_command, reset_command, reset_inline):
        assert "cancel_current_incomplete_session(" in source
