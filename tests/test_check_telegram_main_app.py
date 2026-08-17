import importlib.util
import json
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_telegram_main_app.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("check_telegram_main_app", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self._payload).encode("utf-8")


def test_exit_codes_match_production_acceptance_semantics():
    script = _load_script_module()
    assert script.ENABLED == 0
    assert script.DISABLED == 1
    assert script.UNAVAILABLE == 2


def test_fetch_status_normalizes_successful_get_me(monkeypatch):
    script = _load_script_module()
    requested = {}

    def fake_urlopen(request, *, timeout):
        requested["url"] = request.full_url
        requested["timeout"] = timeout
        return _Response(
            {
                "ok": True,
                "result": {
                    "username": "@milovanovaibot",
                    "has_main_web_app": True,
                },
            }
        )

    monkeypatch.setattr(script, "urlopen", fake_urlopen)

    assert script._fetch_status("secret-token", timeout=3.5) == {
        "username": "milovanovaibot",
        "has_main_web_app": True,
    }
    assert requested["timeout"] == 3.5
    assert requested["url"].endswith("/botsecret-token/getMe")


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"ok": False}, "successful getMe"),
        ({"ok": True, "result": None}, "result is malformed"),
    ],
)
def test_fetch_status_rejects_malformed_provider_payload(monkeypatch, payload, message):
    script = _load_script_module()
    monkeypatch.setattr(script, "urlopen", lambda request, *, timeout: _Response(payload))

    with pytest.raises(RuntimeError, match=message):
        script._fetch_status("secret-token")


def test_main_requires_token_without_contacting_provider(monkeypatch, capsys):
    script = _load_script_module()
    monkeypatch.delenv("BOT_TOKEN", raising=False)
    monkeypatch.setattr(
        script,
        "_fetch_status",
        lambda token: pytest.fail("provider must not be called without BOT_TOKEN"),
    )

    assert script.main() == script.UNAVAILABLE
    assert json.loads(capsys.readouterr().out) == {"error": "BOT_TOKEN is required"}


@pytest.mark.parametrize(
    ("has_main_web_app", "expected_exit"),
    [(True, 0), (False, 1)],
)
def test_main_returns_provider_state_exit_code(
    monkeypatch, capsys, has_main_web_app, expected_exit
):
    script = _load_script_module()
    monkeypatch.setenv("BOT_TOKEN", "secret-token")
    monkeypatch.setattr(
        script,
        "_fetch_status",
        lambda token: {
            "username": "milovanovaibot",
            "has_main_web_app": has_main_web_app,
        },
    )

    assert script.main() == expected_exit
    assert json.loads(capsys.readouterr().out) == {
        "username": "milovanovaibot",
        "has_main_web_app": has_main_web_app,
    }


def test_main_redacts_provider_failure(monkeypatch, capsys):
    script = _load_script_module()
    secret = "secret-token-never-print"
    monkeypatch.setenv("BOT_TOKEN", secret)

    def fail_with_secret(token):
        raise RuntimeError(f"provider failed for {token}")

    monkeypatch.setattr(script, "_fetch_status", fail_with_secret)

    assert script.main() == script.UNAVAILABLE
    output = capsys.readouterr().out
    assert json.loads(output) == {"error": "telegram provider check failed"}
    assert secret not in output
