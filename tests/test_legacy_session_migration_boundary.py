from pathlib import Path


CONTROLLER = (
    Path(__file__).resolve().parents[1] / "telegram_controller.py"
).read_text(encoding="utf-8")


def function(name: str, *, async_def: bool = True) -> str:
    marker = f"{'async ' if async_def else ''}def {name}("
    start = CONTROLLER.index(marker)
    candidates = [
        pos
        for pos in (
            CONTROLLER.find("\nasync def ", start + len(marker)),
            CONTROLLER.find("\ndef ", start + len(marker)),
            CONTROLLER.find("\nclass ", start + len(marker)),
        )
        if pos != -1
    ]
    end = min(candidates) if candidates else len(CONTROLLER)
    return CONTROLLER[start:end]


def _assert_before(source: str, first: str, later: str) -> None:
    assert first in source
    assert later in source
    assert source.index(first) < source.index(later)


def test_strict_session_lifecycle_is_the_only_production_path():
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

    status_lookup = function("_status_session")
    lifecycle_keyboard = function("_lifecycle_keyboard", async_def=False)
    show_active = function("_show_active_attempt")
    start = function("start")
    resume = function("resume_session_handler")
    restart = function("restart_session_handler")
    cancel_session = function("cancel_session_handler")
    cancel_current = function("_cancel_current")
    cancel_quiz = function("cancel_quiz_handler")
    cancel_command = function("cancel")
    reset_command = function("reset_command")
    reset_inline = function("reset_session_inline")
    status_command = function("status_command")
    status_inline = function("show_status_inline")
    reminder = function("remind_unfinished_tests_job")

    # Status classification is centralized but remains strict and durable.
    assert "get_active_quiz_session_strict(" in status_lookup
    assert "classify_restart_session(" in status_lookup
    assert "_status_session(" in start
    assert "_status_session(" in status_command
    assert "_status_session(" in status_inline

    # All lifecycle button producers converge on one attempt-bound builder.
    assert "session_action_payloads(" in lifecycle_keyboard
    assert "_lifecycle_keyboard(" in show_active
    assert "_lifecycle_keyboard(" in start
    assert "_show_active_attempt(" in status_command
    assert "_show_active_attempt(" in status_inline
    assert "_lifecycle_keyboard(" in reminder
    for old_prefix in (
        'callback_data=f"resume_session_',
        'callback_data=f"restart_session_',
        'callback_data=f"cancel_session_',
    ):
        assert old_prefix not in CONTROLLER

    for source, action in (
        (resume, "res"),
        (restart, "rst"),
        (cancel_session, "can"),
    ):
        assert "resolve_session_action(" in source
        assert f'"{action}"' in source

    _assert_before(restart, "resolve_session_action(", "restart_owned_quiz_attempt(")
    assert "expected_attempt_id=" in restart
    assert "cancel_owned_quiz_session(" not in restart

    _assert_before(
        cancel_session,
        "resolve_session_action(",
        "cancel_owned_incomplete_quiz_attempt(",
    )
    assert "expected_attempt_id=" in cancel_session

    # Current-session cancellation is centralized so every command/global exit
    # shares the same completion-safe durable decision.
    assert "cancel_current_incomplete_session(" in cancel_current
    for source in (cancel_quiz, cancel_command, reset_command, reset_inline):
        assert "_cancel_current(" in source
        assert "cancel_active_quiz_session(" not in source

    for old_pattern in (
        'pattern="^resume_session_"',
        'pattern="^restart_session_"',
        'pattern="^cancel_session_"',
    ):
        assert old_pattern not in CONTROLLER
    for new_pattern in (
        'pattern=r"^res:"',
        'pattern=r"^rst:"',
        'pattern=r"^can:"',
    ):
        assert new_pattern in CONTROLLER
