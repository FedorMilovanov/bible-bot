import pytest

import keep_alive


@pytest.fixture
def http(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("ALLOW_DEBUG_AUTH", "true")
    keep_alive.app.config.update(TESTING=True)
    return keep_alive.app.test_client()


def headers(user_id=990001):
    return {"X-Debug-User-Id": str(user_id)}


def test_unauthenticated_malformed_json_gets_canonical_401(http):
    response = http.post(
        "/api/quiz/start",
        data='{"pool_key":',
        content_type="application/json",
    )
    assert response.status_code == 401
    assert response.get_json() == {"error": "telegram authentication required"}


def test_malformed_quiz_json_is_rejected_before_route_logic(http):
    response = http.post(
        "/api/quiz/start",
        data='{"pool_key":',
        content_type="application/json",
        headers=headers(),
    )
    assert response.status_code == 400
    assert response.get_json() == {"error": "invalid JSON"}


@pytest.mark.parametrize("payload", [[], [1, 2], "text", 42, True])
def test_quiz_json_must_be_an_object(http, payload):
    response = http.post(
        "/api/quiz/start",
        json=payload,
        headers=headers(),
    )
    assert response.status_code == 400
    assert response.get_json() == {"error": "JSON object required"}


def test_json_null_must_be_an_object(http):
    # Flask's test client intentionally omits application/json for json=None,
    # so send an actual JSON null payload to exercise the production contract.
    response = http.post(
        "/api/quiz/start",
        data="null",
        content_type="application/json",
        headers=headers(),
    )
    assert response.status_code == 400
    assert response.get_json() == {"error": "JSON object required"}
