import importlib.util
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_production_acceptance.py"


def _load_runner():
    module_name = "run_production_acceptance_profile_gate_test"
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(module_name, None)
    return module


def test_postdeploy_requires_main_app_then_public_profile_provider_gates():
    runner = _load_runner()

    assert "scripts/check_telegram_main_app.py" in runner.POSTDEPLOY_SCRIPTS
    assert "scripts/check_telegram_public_profile.py" in runner.POSTDEPLOY_SCRIPTS
    assert runner.POSTDEPLOY_SCRIPTS.index(
        "scripts/check_telegram_main_app.py"
    ) < runner.POSTDEPLOY_SCRIPTS.index("scripts/check_telegram_public_profile.py")
