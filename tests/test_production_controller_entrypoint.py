import ast
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = (ROOT / "Dockerfile").read_text(encoding="utf-8")
RENDER = (ROOT / "render.yaml").read_text(encoding="utf-8")
ENTRYPOINT = (ROOT / "production_entrypoint.py").read_text(encoding="utf-8")
PRODUCTION = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
QUIZ = (ROOT / "telegram_controller.py").read_text(encoding="utf-8")


def test_runtime_surfaces_run_safe_bootstrap_into_production_composition_root():
    assert 'CMD ["python", "production_entrypoint.py"]' in DOCKERFILE
    assert 'CMD ["python", "bot.py"]' not in DOCKERFILE
    assert 'CMD ["python", "telegram_controller.py"]' not in DOCKERFILE
    assert "startCommand: python production_entrypoint.py" in RENDER
    assert "startCommand: python bot.py" not in RENDER
    assert "startCommand: python telegram_controller.py" not in RENDER
    assert "configure_production_logging()" in ENTRYPOINT
    assert "from telegram_production import main" in ENTRYPOINT
    assert ENTRYPOINT.index("configure_production_logging()") < ENTRYPOINT.index(
        "from telegram_production import main"
    )


def test_quiz_controller_library_is_valid_python_and_owns_quiz_state():
    ast.parse(QUIZ, filename="telegram_controller.py")
    for marker in (
        "launch_quiz_attempt(",
        "apply_live_answer_once(",
        "apply_live_timeout_once(",
        "finalize_live_persisted_attempt(",
        "cancel_current_incomplete_session(",
        "resolve_session_action(",
    ):
        assert marker in QUIZ


def test_production_composition_imports_with_runtime_dependencies():
    env = os.environ.copy()
    env.update(
        {
            "ADMIN_USER_ID": "1",
            "BOT_TOKEN": "123456:TEST_TOKEN",
            "DISABLE_WEB_SERVER": "true",
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import telegram_production as production; "
                "import telegram_admin_controller as admin; "
                "import telegram_controller as quiz; "
                "import telegram_report_controller as reports; "
                "import telegram_battle_controller as battles; "
                "assert callable(production.main); "
                "assert callable(admin.admin_cleanup); "
                "assert callable(quiz.show_results); "
                "assert callable(quiz.quiz_inline_answer); "
                "assert callable(reports.report_confirm); "
                "assert callable(battles.battle_answer)"
            ),
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_legacy_import_temporarily_disables_http_and_restores_environment():
    env = os.environ.copy()
    env.update(
        {
            "ADMIN_USER_ID": "1",
            "BOT_TOKEN": "123456:TEST_TOKEN",
            "DISABLE_WEB_SERVER": "true",
        }
    )
    script = r'''
import os
import telegram_production as production
seen = []

def fake_import(name):
    seen.append((name, os.environ.get("DISABLE_WEB_SERVER")))
    return object()

production.importlib.import_module = fake_import
os.environ.pop("DISABLE_WEB_SERVER", None)
production._import_legacy_presentation()
assert seen == [("bot", "true")]
assert "DISABLE_WEB_SERVER" not in os.environ

seen.clear()
os.environ["DISABLE_WEB_SERVER"] = "false"
production._import_legacy_presentation()
assert seen == [("bot", "true")]
assert os.environ["DISABLE_WEB_SERVER"] == "false"
'''
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_http_server_is_started_only_after_startup_guards_and_handler_setup():
    main_start = PRODUCTION.index("def main() -> None:")
    transport_guard = PRODUCTION.index(
        "validate_telegram_transport_configuration()", main_start
    )
    mongo_guard = PRODUCTION.index("ensure_active_session_unique_index()", main_start)
    miniapp_guard = PRODUCTION.index("if ensure_miniapp_indexes() is not True:", main_start)
    job_queue_guard = PRODUCTION.index("if app.job_queue is None:", main_start)
    last_handler = PRODUCTION.index(
        "app.add_error_handler(errors.build_error_handler(admin_user_id))", main_start
    )
    http_start = PRODUCTION.index("\n    keep_alive()", main_start)
    transport_start = PRODUCTION.index("\n    run_telegram_application(", main_start)

    assert transport_guard < mongo_guard < miniapp_guard < job_queue_guard
    assert job_queue_guard < last_handler < http_start < transport_start


def test_production_startup_requires_explicit_mongo_url():
    assert '_required_env("BOT_TOKEN")' in PRODUCTION
    assert '_required_env("MONGO_URL")' in PRODUCTION
    assert PRODUCTION.index('_required_env("MONGO_URL")') < PRODUCTION.index(
        "Application.builder()"
    )
    assert "- key: MONGO_URL" in RENDER

    env = os.environ.copy()
    env.update(
        {
            "ADMIN_USER_ID": "1",
            "BOT_TOKEN": "123456:TEST_TOKEN",
            "DISABLE_WEB_SERVER": "true",
        }
    )
    env.pop("MONGO_URL", None)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import os; import telegram_production as production; "
                "missing = False; "
                "\ntry:\n production._required_env('MONGO_URL')\n"
                "except ValueError as exc:\n missing = 'MONGO_URL' in str(exc)\n"
                "assert missing; "
                "os.environ['MONGO_URL'] = 'mongodb://configured'; "
                "assert production._required_env('MONGO_URL') == 'mongodb://configured'"
            ),
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_production_composition_wires_recovery_safe_admin_cleanup():
    assert "import telegram_admin_controller as admin" in PRODUCTION
    assert 'CallbackQueryHandler(admin.admin_cleanup, pattern=r"^admin_cleanup$")' in PRODUCTION
    assert (
        'pattern=r"^admin_(hard_questions|active_sessions|broadcast_prompt|back)$"'
        in PRODUCTION
    )
    assert "active_sessions|cleanup|broadcast_prompt" not in PRODUCTION


def test_production_composition_does_not_register_legacy_state_writers():
    ast.parse(PRODUCTION, filename="telegram_production.py")
    for forbidden in (
        "legacy.main()",
        "legacy.test_command",
        "legacy.random_command",
        "legacy.reset_command",
        "legacy.status_command",
        "legacy.relaxed_mode_handler",
        "legacy.timed_mode_handler",
        "legacy.speed_mode_handler",
        "legacy.random_all_start_handler",
        "legacy.intro_start_handler",
        "legacy.retry_errors",
        "legacy.cancel_quiz_handler",
        "legacy.quiz_inline_answer",
        "legacy.challenge_inline_answer",
        "legacy.show_results",
        "legacy.show_challenge_results",
        "legacy.start_battle_questions",
        "legacy.battle_answer",
        "legacy.create_battle",
        "legacy.join_battle",
        "legacy.cancel_battle",
        "legacy.report_confirm",
        "legacy.report_inaccuracy_handler",
    ):
        assert forbidden not in PRODUCTION
