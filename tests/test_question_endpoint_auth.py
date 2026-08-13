import pytest

import keep_alive
import web_api


@pytest.fixture
def http(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("ALLOW_DEBUG_AUTH", "true")
    keep_alive.app.config.update(TESTING=True)
    return keep_alive.app.test_client()


def test_question_compatibility_endpoint_requires_telegram_auth(http):
    response = http.get("/api/questions/easy_p1")

    assert response.status_code == 401
    assert response.get_json() == {"error": "telegram authentication required"}


def test_question_compatibility_endpoint_exposes_no_answer_metadata(http):
    response = http.get(
        "/api/questions/easy_p1",
        headers={"X-Debug-User-Id": "991101"},
    )

    assert response.status_code == 200
    questions = response.get_json()
    assert questions
    assert set(questions[0]) == {"id", "question", "options"}


def test_question_compatibility_endpoint_is_rate_limited_per_user(http, monkeypatch):
    monkeypatch.setattr(web_api, "_QUESTION_ENDPOINT_LIMIT", (1, 60))
    headers = {"X-Debug-User-Id": "991102"}

    first = http.get("/api/questions/easy_p1", headers=headers)
    second = http.get("/api/questions/easy_p1", headers=headers)

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.get_json()["error"] == "rate limit exceeded"
    assert int(second.headers["Retry-After"]) >= 1
