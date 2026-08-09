from flask import Flask

from web_api.auth import get_user_from_request


def _resolve_debug_user(monkeypatch, *, render: str):
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("ALLOW_DEBUG_AUTH", "true")
    monkeypatch.setenv("RENDER", render)
    monkeypatch.delenv("BOT_TOKEN", raising=False)

    app = Flask(__name__)
    with app.test_request_context(headers={"X-Debug-User-Id": "991201"}):
        return get_user_from_request()


def test_debug_auth_remains_available_for_explicit_local_development(monkeypatch):
    user = _resolve_debug_user(monkeypatch, render="false")

    assert user == {"id": 991201, "first_name": "Local Debug User"}


def test_debug_auth_is_disabled_on_render_even_if_flags_are_misconfigured(monkeypatch):
    user = _resolve_debug_user(monkeypatch, render="true")

    assert user is None
