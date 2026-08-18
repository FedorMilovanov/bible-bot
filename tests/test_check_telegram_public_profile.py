import importlib.util
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

import telegram_public_profile as profile


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_telegram_public_profile.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("check_telegram_public_profile", SCRIPT_PATH)
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
    assert script.SAFE == 0
    assert script.UNSAFE == 1
    assert script.UNAVAILABLE == 2


def test_fetch_value_uses_read_only_provider_method_and_locale(monkeypatch):
    script = _load_script_module()
    requested = {}

    def fake_urlopen(request, *, timeout):
        requested["url"] = request.full_url
        requested["method"] = request.method
        requested["timeout"] = timeout
        return _Response({"ok": True, "result": {"name": profile.PUBLIC_BOT_NAME}})

    monkeypatch.setattr(script, "urlopen", fake_urlopen)

    assert (
        script._fetch_value(
            "secret-token", "getMyName", "name", "ru", timeout=3.5
        )
        == profile.PUBLIC_BOT_NAME
    )
    parsed = urlsplit(requested["url"])
    assert parsed.path.endswith("/botsecret-token/getMyName")
    assert parse_qs(parsed.query) == {"language_code": ["ru"]}
    assert requested["method"] == "GET"
    assert requested["timeout"] == 3.5


def test_default_locale_omits_language_query(monkeypatch):
    script = _load_script_module()
    requested = {}

    def fake_urlopen(request, *, timeout):
        requested["url"] = request.full_url
        return _Response({"ok": True, "result": {"description": "ok"}})

    monkeypatch.setattr(script, "urlopen", fake_urlopen)

    assert script._fetch_value("secret", "getMyDescription", "description", "") == "ok"
    assert urlsplit(requested["url"]).query == ""


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"ok": False}, "successful getMyName"),
        ({"ok": True, "result": None}, "result is malformed"),
        ({"ok": True, "result": {"name": None}}, "field is malformed"),
    ],
)
def test_fetch_value_rejects_malformed_provider_payload(monkeypatch, payload, message):
    script = _load_script_module()
    monkeypatch.setattr(script, "urlopen", lambda request, *, timeout: _Response(payload))

    with pytest.raises(RuntimeError, match=message):
        script._fetch_value("secret-token", "getMyName", "name", "")


def test_profile_mismatches_checks_both_default_and_russian(monkeypatch):
    script = _load_script_module()
    calls = []

    def fake_fetch(token, method, result_key, language_code, *, timeout=5.0):
        del token, timeout
        calls.append((method, result_key, language_code))
        expected = {
            "name": profile.PUBLIC_BOT_NAME,
            "short_description": profile.PUBLIC_BOT_SHORT_DESCRIPTION,
            "description": profile.PUBLIC_BOT_DESCRIPTION,
        }[result_key]
        if language_code == "ru" and result_key == "description":
            return "старое описание"
        return expected

    monkeypatch.setattr(script, "_fetch_value", fake_fetch)

    assert script._profile_mismatches("secret-token") == ["ru:description"]
    assert len(calls) == 6


def test_main_requires_token_without_contacting_provider(monkeypatch, capsys):
    script = _load_script_module()
    monkeypatch.delenv("BOT_TOKEN", raising=False)
    monkeypatch.setattr(
        script,
        "_profile_mismatches",
        lambda token: pytest.fail("provider must not be called without BOT_TOKEN"),
    )

    assert script.main() == script.UNAVAILABLE
    assert json.loads(capsys.readouterr().out) == {"error": "BOT_TOKEN is required"}


@pytest.mark.parametrize(
    ("mismatches", "expected_exit"),
    [([], 0), (["ru:name"], 1)],
)
def test_main_returns_profile_contract_exit_code(
    monkeypatch, capsys, mismatches, expected_exit
):
    script = _load_script_module()
    monkeypatch.setenv("BOT_TOKEN", "secret-token")
    monkeypatch.setattr(script, "_profile_mismatches", lambda token: mismatches)

    assert script.main() == expected_exit
    assert json.loads(capsys.readouterr().out) == {
        "locales": ["default", "ru"],
        "mismatches": mismatches,
        "profile_ok": not mismatches,
    }


def test_main_redacts_provider_failure(monkeypatch, capsys):
    script = _load_script_module()
    secret = "secret-token-never-print"
    monkeypatch.setenv("BOT_TOKEN", secret)

    def fail_with_secret(token):
        raise RuntimeError(f"provider failed for {token}")

    monkeypatch.setattr(script, "_profile_mismatches", fail_with_secret)

    assert script.main() == script.UNAVAILABLE
    output = capsys.readouterr().out
    assert json.loads(output) == {"error": "telegram public profile check failed"}
    assert secret not in output
