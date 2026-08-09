import pytest

import web_api
from web_api import telegram_transport


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "123456:TEST_TOKEN")
    monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", "https://example.com")
    monkeypatch.delenv("TELEGRAM_WEBHOOK_SECRET", raising=False)
    telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.clear()
    flask_app = web_api.create_app()
    flask_app.config.update(TESTING=True)
    return flask_app


def _secret_headers():
    return {
        "X-Telegram-Bot-Api-Secret-Token": telegram_transport.telegram_webhook_secret(),
    }


def test_webhook_route_is_hidden_in_polling_mode(app, monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "polling")

    response = app.test_client().post(
        telegram_transport.WEBHOOK_PATH,
        json={"update_id": 1},
        headers=_secret_headers(),
    )

    assert response.status_code == 404


def test_webhook_rejects_missing_or_wrong_secret_before_json_parsing(app, monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "webhook")
    http = app.test_client()

    missing = http.post(
        telegram_transport.WEBHOOK_PATH,
        data='{"update_id":',
        content_type="application/json",
    )
    wrong = http.post(
        telegram_transport.WEBHOOK_PATH,
        data='{"update_id":',
        content_type="application/json",
        headers={"X-Telegram-Bot-Api-Secret-Token": "wrong-secret-value"},
    )

    assert missing.status_code == 401
    assert wrong.status_code == 401


def test_webhook_rejects_invalid_json_after_valid_secret(app, monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "webhook")

    response = app.test_client().post(
        telegram_transport.WEBHOOK_PATH,
        data='{"update_id":',
        content_type="application/json",
        headers=_secret_headers(),
    )

    assert response.status_code == 400
    assert response.get_json() == {"error": "invalid JSON"}


def test_webhook_returns_503_until_ptb_bridge_is_ready(app, monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "webhook")

    response = app.test_client().post(
        telegram_transport.WEBHOOK_PATH,
        json={"update_id": 1},
        headers=_secret_headers(),
    )

    assert response.status_code == 503
    assert response.get_json() == {"error": "telegram application not ready"}
    assert response.headers["Cache-Control"] == "no-store"


def test_valid_webhook_payload_is_forwarded_to_bridge(app, monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "webhook")
    captured = []
    monkeypatch.setattr(telegram_transport.TELEGRAM_WEBHOOK_BRIDGE, "submit", captured.append)

    payload = {"update_id": 42, "message": {"message_id": 1}}
    response = app.test_client().post(
        telegram_transport.WEBHOOK_PATH,
        json=payload,
        headers=_secret_headers(),
    )

    assert response.status_code == 200
    assert response.get_json() == {"ok": True}
    assert captured == [payload]


def test_invalid_telegram_update_returns_400(app, monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "webhook")

    def reject(_payload):
        raise telegram_transport.InvalidWebhookUpdate("bad update")

    monkeypatch.setattr(telegram_transport.TELEGRAM_WEBHOOK_BRIDGE, "submit", reject)
    response = app.test_client().post(
        telegram_transport.WEBHOOK_PATH,
        json={"not": "an update"},
        headers=_secret_headers(),
    )

    assert response.status_code == 400
    assert response.get_json() == {"error": "invalid telegram update"}
