import pytest

import keep_alive
import web_api


@pytest.fixture
def http(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("ALLOW_DEBUG_AUTH", "true")
    monkeypatch.setenv("RENDER", "false")
    keep_alive.app.config.update(TESTING=True)
    return keep_alive.app.test_client()


def test_question_rate_limit_is_shared_across_pool_paths(http, monkeypatch):
    monkeypatch.setattr(web_api, "_QUESTION_ENDPOINT_LIMIT", (1, 60))
    headers = {"X-Debug-User-Id": "991301"}

    first = http.get("/api/questions/easy_p1", headers=headers)
    switched_pool = http.get("/api/questions/medium_p1", headers=headers)

    assert first.status_code == 200
    assert switched_pool.status_code == 429
    assert switched_pool.get_json()["error"] == "rate limit exceeded"
