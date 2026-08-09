import os

import pytest

import database
import keep_alive
import web_api
from web_api import auth as auth_module
from web_api import db_hardening
from web_api.rate_limit import SlidingWindowLimiter


@pytest.fixture
def http(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("ALLOW_DEBUG_AUTH", "true")
    keep_alive.app.config.update(TESTING=True)
    return keep_alive.app.test_client()


def debug_headers(user_id=987654321):
    return {"X-Debug-User-Id": str(user_id)}


def test_api_requires_json_before_quiz_work(http):
    response = http.post(
        "/api/quiz/start",
        data="{}",
        content_type="text/plain",
        headers=debug_headers(),
    )
    assert response.status_code == 415
    assert response.get_json()["error"] == "application/json required"


def test_request_body_limit_returns_json_413(http):
    payload = '{"padding":"' + ("x" * (70 * 1024)) + '"}'
    response = http.post(
        "/api/quiz/start",
        data=payload,
        content_type="application/json",
        headers=debug_headers(),
    )
    assert response.status_code == 413
    assert response.get_json()["error"] == "request body too large"


def test_security_and_cache_headers(http):
    public = http.get("/live")
    assert public.status_code == 200
    assert public.headers["X-Content-Type-Options"] == "nosniff"
    assert public.headers["Referrer-Policy"] == "no-referrer"
    assert "no-cache" in public.headers["Cache-Control"]

    api = http.get("/api/me", headers=debug_headers())
    assert api.headers["X-Content-Type-Options"] == "nosniff"
    assert api.headers["Cache-Control"] == "no-store"
    assert api.headers["Pragma"] == "no-cache"


def test_rate_limit_returns_retry_after(http, monkeypatch):
    monkeypatch.setitem(web_api._RATE_LIMITS, ("GET", "/api/me"), (2, 60))
    headers = debug_headers(987654322)

    http.get("/api/me", headers=headers)
    http.get("/api/me", headers=headers)
    limited = http.get("/api/me", headers=headers)

    assert limited.status_code == 429
    assert limited.get_json()["error"] == "rate limit exceeded"
    assert int(limited.headers["Retry-After"]) >= 1


def test_sliding_window_limiter_is_scoped_by_key():
    limiter = SlidingWindowLimiter()
    assert limiter.allow("u1:start", limit=1, window_seconds=60)[0] is True
    allowed, retry_after = limiter.allow("u1:start", limit=1, window_seconds=60)
    assert allowed is False
    assert retry_after >= 1
    assert limiter.allow("u2:start", limit=1, window_seconds=60)[0] is True


def test_oversized_init_data_is_rejected(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "123456:TEST")
    assert auth_module.verify_init_data("x" * (auth_module.MAX_INIT_DATA_LENGTH + 1)) is None


class FakeCollection:
    def __init__(self):
        self.indexes = []

    def create_index(self, keys, **kwargs):
        self.indexes.append((keys, kwargs))
        return kwargs.get("name", "idx")


class FakeDB:
    def __init__(self, collection):
        self.collection = collection

    def __getitem__(self, name):
        assert name == "miniapp_sessions"
        return self.collection


def test_miniapp_db_indexes_include_unique_active_user(monkeypatch):
    collection = FakeCollection()
    monkeypatch.setattr(database, "db", FakeDB(collection), raising=False)
    monkeypatch.setattr(db_hardening, "_INDEXES_READY", False)

    assert db_hardening.ensure_miniapp_indexes() is True
    unique = [kwargs for _keys, kwargs in collection.indexes if kwargs.get("name") == "uniq_miniapp_active_user"]
    assert unique == [{
        "unique": True,
        "partialFilterExpression": {"status": "in_progress"},
        "name": "uniq_miniapp_active_user",
    }]


def test_waitress_is_given_bounded_request_limits(monkeypatch):
    import waitress

    captured = {}

    def fake_serve(app, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(waitress, "serve", fake_serve)
    monkeypatch.setenv("PORT", "18080")
    monkeypatch.setenv("WEB_THREADS", "3")
    monkeypatch.setenv("MAX_REQUEST_BODY_BYTES", "65536")
    monkeypatch.setenv("MAX_REQUEST_HEADER_BYTES", "65536")

    keep_alive.run()

    assert captured["host"] == "0.0.0.0"
    assert captured["port"] == 18080
    assert captured["threads"] == 3
    assert captured["max_request_body_size"] == 65536
    assert captured["max_request_header_size"] == 65536
    assert captured["clear_untrusted_proxy_headers"] is True
    assert captured["expose_tracebacks"] is False


def test_css_uses_telegram_theme_safe_area_and_reduced_motion():
    css = (os.path.dirname(os.path.dirname(__file__)) + "/miniapp/style.css")
    with open(css, encoding="utf-8") as handle:
        content = handle.read()
    assert "--tg-theme-bg-color" in content
    assert "--tg-content-safe-area-inset-bottom" in content
    assert "prefers-reduced-motion:reduce" in content
