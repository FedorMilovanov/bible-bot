import database
import keep_alive
from web_api import TELEGRAM_WEBHOOK_BRIDGE


def _client():
    keep_alive.app.config.update(TESTING=True)
    return keep_alive.app.test_client()


def test_production_ready_requires_database_and_webhook_bridge(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "webhook")
    monkeypatch.setattr(database, "check_db_connection", lambda: True)
    monkeypatch.setattr(TELEGRAM_WEBHOOK_BRIDGE, "ready", lambda: True)

    response = _client().get("/production/ready")

    assert response.status_code == 200
    assert response.get_json() == {
        "status": "ready",
        "database": True,
        "telegram": True,
        "transport": "webhook",
    }
    assert response.headers["Cache-Control"] == "no-store"


def test_production_ready_fails_when_database_is_unavailable(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "webhook")
    monkeypatch.setattr(database, "check_db_connection", lambda: False)
    monkeypatch.setattr(TELEGRAM_WEBHOOK_BRIDGE, "ready", lambda: True)

    response = _client().get("/production/ready")

    assert response.status_code == 503
    assert response.get_json()["database"] is False
    assert response.get_json()["telegram"] is True


def test_production_ready_fails_until_webhook_application_is_started(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "webhook")
    monkeypatch.setattr(database, "check_db_connection", lambda: True)
    monkeypatch.setattr(TELEGRAM_WEBHOOK_BRIDGE, "ready", lambda: False)

    response = _client().get("/production/ready")

    assert response.status_code == 503
    assert response.get_json()["database"] is True
    assert response.get_json()["telegram"] is False


def test_production_ready_accepts_polling_rollback_when_database_is_ready(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "polling")
    monkeypatch.setattr(database, "check_db_connection", lambda: True)
    monkeypatch.setattr(TELEGRAM_WEBHOOK_BRIDGE, "ready", lambda: False)

    response = _client().get("/production/ready")

    assert response.status_code == 200
    assert response.get_json()["transport"] == "polling"
    assert response.get_json()["telegram"] is True


def test_production_ready_fails_closed_on_invalid_transport(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "invalid")
    monkeypatch.setattr(database, "check_db_connection", lambda: True)

    response = _client().get("/production/ready")

    assert response.status_code == 503
    assert response.get_json() == {
        "status": "not_ready",
        "database": True,
        "telegram": False,
        "transport": "invalid",
    }
