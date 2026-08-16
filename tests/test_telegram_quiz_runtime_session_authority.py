from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = (ROOT / "telegram_quiz_runtime_controller.py").read_text(encoding="utf-8")
CHALLENGE = (ROOT / "telegram_challenge_controller.py").read_text(encoding="utf-8")
PRODUCTION = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def function(source: str, name: str, *, async_def: bool = True) -> str:
    marker = f"{'async ' if async_def else ''}def {name}("
    start = source.index(marker)
    candidates = [
        pos
        for pos in (
            source.find("\nasync def ", start + len(marker)),
            source.find("\ndef ", start + len(marker)),
            source.find("\nclass ", start + len(marker)),
        )
        if pos != -1
    ]
    end = min(candidates) if candidates else len(source)
    return source[start:end]


def _assert_before(source: str, first: str, later: str) -> None:
    assert first in source
    assert later in source
    assert source.index(first) < source.index(later)


def test_quiz_runtime_owns_strict_resume_cancel_and_status_lifecycle():
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

    status_lookup = function(RUNTIME, "_status_session")
    lifecycle_keyboard = function(RUNTIME, "_lifecycle_keyboard", async_def=False)
    show_active = function(RUNTIME, "_show_active_attempt")
    start = function(RUNTIME, "start")
    resume = function(RUNTIME, "resume_session_handler")
    cancel_session = function(RUNTIME, "cancel_session_handler")
    cancel_current = function(RUNTIME, "_cancel_current")
    cancel_quiz = function(RUNTIME, "cancel_quiz_handler")
    cancel_command = function(RUNTIME, "cancel")
    reset_command = function(RUNTIME, "reset_command")
    reset_inline = function(RUNTIME, "reset_session_inline")
    status_command = function(RUNTIME, "status_command")
    status_inline = function(RUNTIME, "show_status_inline")
    reminder = function(RUNTIME, "remind_unfinished_tests_job")

    assert "get_active_quiz_session_strict(" in status_lookup
    assert "classify_restart_session(" in status_lookup
    assert "_status_session(" in start
    assert "_status_session(" in status_command
    assert "_status_session(" in status_inline

    assert "session_action_payloads(" in lifecycle_keyboard
    assert "_lifecycle_keyboard(" in show_active
    assert "_lifecycle_keyboard(" in start
    assert "_show_active_attempt(" in status_command
    assert "_show_active_attempt(" in status_inline
    assert "_lifecycle_keyboard(" in reminder

    assert "resolve_session_action(" in resume
    assert '"res"' in resume
    assert "resolve_session_action(" in cancel_session
    assert '"can"' in cancel_session
    _assert_before(
        cancel_session,
        "resolve_session_action(",
        "cancel_owned_incomplete_quiz_attempt(",
    )
    assert "expected_attempt_id=" in cancel_session

    assert "cancel_current_incomplete_session(" in cancel_current
    for source in (cancel_quiz, cancel_command, reset_command, reset_inline):
        assert "_cancel_current(" in source
        assert "cancel_active_quiz_session(" not in source


def test_challenge_controller_owns_attempt_bound_restart():
    restart = function(CHALLENGE, "restart_session_handler")
    _assert_before(restart, "resolve_session_action(", "restart_owned_quiz_attempt,")
    assert '"rst"' in restart
    assert "expected_attempt_id=resolved.attempt_id" in restart
    assert "cancel_owned_quiz_session(" not in restart


def test_production_handler_graph_uses_attempt_bound_lifecycle_protocol():
    for old_pattern in (
        'pattern="^resume_session_"',
        'pattern="^restart_session_"',
        'pattern="^cancel_session_"',
    ):
        assert old_pattern not in PRODUCTION

    assert 'CallbackQueryHandler(quiz.resume_session_handler, pattern=r"^res:")' in PRODUCTION
    assert 'CallbackQueryHandler(challenge.restart_session_handler, pattern=r"^rst:")' in PRODUCTION
    assert 'CallbackQueryHandler(quiz.cancel_session_handler, pattern=r"^can:")' in PRODUCTION
