import importlib.util
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_production_acceptance.py"


def _load_runner():
    spec = importlib.util.spec_from_file_location("run_production_acceptance", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_postdeploy_requires_main_app_then_public_profile_provider_gates():
    runner = _load_runner()

    assert "scripts/check_telegram_main_app.py" in runner.POSTDEPLOY_SCRIPTS
    assert "scripts/check_telegram_public_profile.py" in runner.POSTDEPLOY_SCRIPTS
    assert runner.POSTDEPLOY_SCRIPTS.index(
        "scripts/check_telegram_main_app.py"
    ) < runner.POSTDEPLOY_SCRIPTS.index("scripts/check_telegram_public_profile.py")
