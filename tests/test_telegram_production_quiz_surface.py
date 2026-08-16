from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")

# Keep this deliberately exact. telegram_controller.py remains a transitional
# compatibility module; production may use only the audited Mongo-authoritative
# entrypoints below. New quiz.<attr> access requires an explicit boundary review.
EXPECTED_PRODUCTION_QUIZ_ATTRIBUTES = {
    "_general_message_fallback",
    "_save_all_sessions",
    "cancel",
    "cancel_quiz_handler",
    "cancel_session_handler",
    "challenge_inline_answer",
    "quiz_inline_answer",
    "random_all_start_handler",
    "random_command",
    "remind_unfinished_tests_job",
    "reset_command",
    "reset_session_inline",
    "resume_session_handler",
    "show_status_inline",
    "start",
    "status_command",
}

FORBIDDEN_STANDALONE_OR_LEGACY_QUIZ_ATTRIBUTES = {
    "admin_command",
    "back_to_main",
    "choose_level",
    "intro_start_handler",
    "level_selected",
    "main",
    "restart_session_handler",
    "run",
    "send_final_results_menu",
    "test_command",
}


def _production_quiz_attributes() -> set[str]:
    tree = ast.parse(PRODUCTION_SOURCE, filename="telegram_production.py")
    return {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "quiz"
    }


def test_production_quiz_controller_surface_is_exactly_frozen():
    assert _production_quiz_attributes() == EXPECTED_PRODUCTION_QUIZ_ATTRIBUTES


def test_production_cannot_reregister_standalone_or_legacy_quiz_entrypoints():
    observed = _production_quiz_attributes()
    assert observed.isdisjoint(FORBIDDEN_STANDALONE_OR_LEGACY_QUIZ_ATTRIBUTES)
    for attribute in FORBIDDEN_STANDALONE_OR_LEGACY_QUIZ_ATTRIBUTES:
        assert f"quiz.{attribute}" not in PRODUCTION_SOURCE


def test_restart_is_owned_by_focused_controller_in_production():
    tree = ast.parse(PRODUCTION_SOURCE, filename="telegram_production.py")
    restart_handlers = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not (
            isinstance(node.func, ast.Name)
            and node.func.id == "CallbackQueryHandler"
            and node.args
        ):
            continue
        callback = node.args[0]
        pattern = next(
            (
                keyword.value.value
                for keyword in node.keywords
                if keyword.arg == "pattern"
                and isinstance(keyword.value, ast.Constant)
                and isinstance(keyword.value.value, str)
            ),
            None,
        )
        if pattern == r"^rst:":
            restart_handlers.append(callback)

    assert len(restart_handlers) == 1
    callback = restart_handlers[0]
    assert isinstance(callback, ast.Attribute)
    assert isinstance(callback.value, ast.Name)
    assert callback.value.id == "challenge"
    assert callback.attr == "restart_session_handler"
