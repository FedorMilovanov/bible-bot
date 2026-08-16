import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
COMPAT = (ROOT / "telegram_controller.py").read_text(encoding="utf-8")
CORE = (ROOT / "telegram_quiz_controller.py").read_text(encoding="utf-8")
RUNTIME = (ROOT / "telegram_quiz_runtime_state.py").read_text(encoding="utf-8")


def test_production_quiz_core_is_bot_free_and_not_a_standalone_app():
    for source in (PRODUCTION, COMPAT, CORE, RUNTIME):
        assert "import bot" not in source
        assert "from bot" not in source
    for source in (PRODUCTION, COMPAT, CORE):
        assert "legacy." not in source

    # Runtime state keeps a migration utility for non-production/manual legacy
    # callers, but the production composition root no longer installs it.
    assert "def install_legacy_bridge(legacy_module)" in RUNTIME
    assert "legacy_module.user_data = _user_data" in RUNTIME
    assert "install_legacy_bridge(legacy)" not in PRODUCTION

    assert "run_polling" not in CORE
    assert "Application.builder" not in CORE
    assert "def main(" not in CORE
    assert "_import_legacy_presentation" not in PRODUCTION
    assert "importlib" not in PRODUCTION


def test_compatibility_name_returns_exact_core_module_object():
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
                "import telegram_controller as compat; "
                "import telegram_quiz_controller as core; "
                "assert compat is core; "
                "assert compat.user_data is core.user_data; "
                "assert compat._render_result is core._render_result"
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


def test_production_import_graph_does_not_load_bot_module():
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
                "import sys; "
                "assert 'bot' not in sys.modules; "
                "import telegram_production as production; "
                "assert 'bot' not in sys.modules; "
                "import telegram_quiz_controller as core; "
                "assert production.quiz is core; "
                "assert callable(production.main)"
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


def test_production_root_uses_direct_canonical_runtime_configuration():
    assert "import telegram_quiz_controller as quiz" in PRODUCTION
    assert "main_menu.configure_miniapp_url_provider(_miniapp_url)" in PRODUCTION
    assert "result_delivery.install_result_card_renderer(quiz)" in PRODUCTION
    assert "install_legacy_bridge(legacy)" not in PRODUCTION
    assert "legacy =" not in PRODUCTION
