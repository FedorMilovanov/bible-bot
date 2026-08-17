import importlib.util
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_production_acceptance.py"
MAIN_APP_CHECK = "scripts/check_telegram_main_app.py"


def _load_runner():
    spec = importlib.util.spec_from_file_location("run_production_acceptance", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_main_mini_app_check_is_mandatory_postdeploy_only():
    runner = _load_runner()

    assert MAIN_APP_CHECK not in runner.PREDEPLOY_SCRIPTS
    assert runner.POSTDEPLOY_SCRIPTS.count(MAIN_APP_CHECK) == 1
    assert runner.POSTDEPLOY_SCRIPTS.index(MAIN_APP_CHECK) > runner.POSTDEPLOY_SCRIPTS.index(
        "scripts/check_telegram_webhook.py"
    )


def test_disabled_main_mini_app_makes_postdeploy_unsafe(monkeypatch):
    runner = _load_runner()
    monkeypatch.setattr(
        runner,
        "_http_contracts",
        lambda: [runner.CheckResult("deployment_http", runner.SAFE, "exact revision")],
    )

    def fake_run_script(path):
        if path == MAIN_APP_CHECK:
            return runner.CheckResult(
                path,
                runner.UNSAFE,
                '{"has_main_web_app": false, "username": "milovanovaibot"}',
            )
        return runner.CheckResult(path, runner.SAFE, "safe")

    monkeypatch.setattr(runner, "_run_script", fake_run_script)

    code, results = runner.run_postdeploy()

    assert code == runner.UNSAFE
    assert any(item.name == MAIN_APP_CHECK and item.code == runner.UNSAFE for item in results)


def test_unavailable_main_mini_app_state_keeps_postdeploy_unavailable(monkeypatch):
    runner = _load_runner()
    monkeypatch.setattr(
        runner,
        "_http_contracts",
        lambda: [runner.CheckResult("deployment_http", runner.SAFE, "exact revision")],
    )

    def fake_run_script(path):
        if path == MAIN_APP_CHECK:
            return runner.CheckResult(path, runner.UNAVAILABLE, "provider unavailable")
        return runner.CheckResult(path, runner.SAFE, "safe")

    monkeypatch.setattr(runner, "_run_script", fake_run_script)

    code, _ = runner.run_postdeploy()

    assert code == runner.UNAVAILABLE
