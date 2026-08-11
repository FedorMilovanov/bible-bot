import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = (ROOT / "Dockerfile").read_text(encoding="utf-8")
CONTROLLER = (ROOT / "telegram_controller.py").read_text(encoding="utf-8")


def test_docker_runs_strict_telegram_controller():
    assert 'CMD ["python", "telegram_controller.py"]' in DOCKERFILE
    assert 'CMD ["python", "bot.py"]' not in DOCKERFILE


def test_production_controller_is_valid_python_and_owns_quiz_state():
    ast.parse(CONTROLLER, filename="telegram_controller.py")
    for marker in (
        "launch_quiz_attempt(",
        "apply_live_answer_once(",
        "apply_live_timeout_once(",
        "finalize_live_persisted_attempt(",
        "cancel_current_incomplete_session(",
        "resolve_session_action(",
    ):
        assert marker in CONTROLLER


def test_legacy_bot_is_helper_only_for_quiz_runtime():
    for forbidden in (
        "legacy.main()",
        "legacy.quiz_inline_answer",
        "legacy.challenge_inline_answer",
        "legacy.show_results",
        "legacy.show_challenge_results",
        "legacy.resume_session_handler",
        "legacy.restart_session_handler",
        "legacy.cancel_session_handler",
    ):
        assert forbidden not in CONTROLLER
