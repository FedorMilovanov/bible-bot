import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "check_telegram_webhook.py"
SPEC = importlib.util.spec_from_file_location("check_telegram_webhook", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
preflight = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(preflight)


def _safe_info(**overrides):
    info = {
        "url": "https://example.com/telegram/webhook",
        "max_connections": 1,
        "allowed_updates": ["message", "callback_query"],
        "pending_update_count": 0,
    }
    info.update(overrides)
    return info


def test_classify_accepts_exact_webhook_contract():
    errors, warnings = preflight._classify(
        _safe_info(),
        expected_url="https://example.com/telegram/webhook",
        now=1_000,
    )
    assert errors == []
    assert warnings == []


@pytest.mark.parametrize(
    ("overrides", "needle"),
    [
        ({"url": "https://wrong.example/telegram/webhook"}, "webhook URL"),
        ({"max_connections": 4}, "max_connections"),
        ({"allowed_updates": ["message"]}, "allowed_updates"),
        ({"pending_update_count": -1}, "pending_update_count"),
    ],
)
def test_classify_rejects_unsafe_contract(overrides, needle):
    errors, _warnings = preflight._classify(
        _safe_info(**overrides),
        expected_url="https://example.com/telegram/webhook",
        now=1_000,
    )
    assert any(needle in item for item in errors)


def test_recent_delivery_error_is_unsafe_but_old_error_is_warning():
    recent_errors, recent_warnings = preflight._classify(
        _safe_info(last_error_date=950, last_error_message="gateway failed"),
        expected_url="https://example.com/telegram/webhook",
        now=1_000,
        error_max_age_seconds=300,
    )
    assert any("recent delivery error" in item for item in recent_errors)
    assert recent_warnings == []

    old_errors, old_warnings = preflight._classify(
        _safe_info(last_error_date=100, last_error_message="old gateway failure"),
        expected_url="https://example.com/telegram/webhook",
        now=1_000,
        error_max_age_seconds=300,
    )
    assert old_errors == []
    assert any("older delivery error" in item for item in old_warnings)


def test_pending_updates_are_reported_without_becoming_false_failure():
    errors, warnings = preflight._classify(
        _safe_info(pending_update_count=3),
        expected_url="https://example.com/telegram/webhook",
        now=1_000,
    )
    assert errors == []
    assert warnings == ["Telegram currently reports 3 pending update(s)"]


def test_main_returns_safe_for_exact_state(monkeypatch, capsys):
    monkeypatch.setenv("BOT_TOKEN", "123456:SECRET_TOKEN")
    monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", "https://example.com")
    monkeypatch.delenv("RENDER_EXTERNAL_URL", raising=False)
    monkeypatch.setattr(preflight, "_fetch_info", lambda _token: _safe_info())

    assert preflight.main() == preflight.SAFE
    output = capsys.readouterr().out
    assert "SAFE:" in output
    assert "SECRET_TOKEN" not in output


def test_main_returns_unsafe_for_mismatch(monkeypatch, capsys):
    monkeypatch.setenv("BOT_TOKEN", "123456:SECRET_TOKEN")
    monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", "https://example.com")
    monkeypatch.setattr(
        preflight,
        "_fetch_info",
        lambda _token: _safe_info(max_connections=2),
    )

    assert preflight.main() == preflight.UNSAFE
    output = capsys.readouterr().out
    assert "UNSAFE:" in output
    assert "SECRET_TOKEN" not in output


def test_main_returns_unavailable_without_token_before_network(monkeypatch, capsys):
    monkeypatch.delenv("BOT_TOKEN", raising=False)

    def should_not_fetch(_token):  # pragma: no cover - failure path only
        raise AssertionError("network must not be called")

    monkeypatch.setattr(preflight, "_fetch_info", should_not_fetch)
    assert preflight.main() == preflight.UNAVAILABLE
    assert "UNAVAILABLE:" in capsys.readouterr().out


def test_network_failure_never_echoes_bot_token(monkeypatch, capsys):
    token = "123456:TOP_SECRET"
    monkeypatch.setenv("BOT_TOKEN", token)
    monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", "https://example.com")

    def fail(_token):
        raise RuntimeError(f"request https://api.telegram.org/bot{token}/getWebhookInfo failed")

    monkeypatch.setattr(preflight, "_fetch_info", fail)
    assert preflight.main() == preflight.UNAVAILABLE
    assert token not in capsys.readouterr().out


def test_preflight_is_read_only_and_matches_runtime_transport_contract():
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    transport = (ROOT / "web_api" / "telegram_transport.py").read_text(encoding="utf-8")

    forbidden = (
        "setWebhook",
        "deleteWebhook",
        "set_webhook",
        "delete_webhook",
        "MongoClient",
        "update_one(",
        "delete_one(",
        "create_index(",
        "drop_index(",
    )
    assert all(token not in source for token in forbidden)
    assert 'EXPECTED_MAX_CONNECTIONS = 1' in source
    assert 'EXPECTED_ALLOWED_UPDATES = frozenset({"message", "callback_query"})' in source
    assert 'WEBHOOK_ALLOWED_UPDATES = ("message", "callback_query")' in transport
    assert 'TELEGRAM_WEBHOOK_MAX_CONNECTIONS", "1"' in transport


def test_expected_url_uses_https_origin_only(monkeypatch):
    monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", "https://example.com/")
    assert preflight._expected_webhook_url() == "https://example.com/telegram/webhook"

    for invalid in (
        "http://example.com",
        "https://user@example.com",
        "https://example.com/path",
        "https://example.com?x=1",
    ):
        monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", invalid)
        with pytest.raises(ValueError):
            preflight._expected_webhook_url()
