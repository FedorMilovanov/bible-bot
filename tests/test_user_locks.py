import pytest

import keep_alive
from web_api import routes as routes_module
from web_api.user_locks import user_operation_lock


@pytest.fixture
def http(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("ALLOW_DEBUG_AUTH", "true")
    keep_alive.app.config.update(TESTING=True)
    return keep_alive.app.test_client()


def debug_headers(user_id):
    return {"X-Debug-User-Id": str(user_id)}


def test_user_lock_mapping_is_stable_and_bounded():
    assert user_operation_lock(123) is user_operation_lock("123")
    assert user_operation_lock(123).locked() is False
    with pytest.raises(ValueError):
        user_operation_lock("not-a-user-id")


def test_quiz_routes_hold_same_user_lock_for_full_request(http, monkeypatch):
    user_id = 987650001
    observed = []

    def fake_operation(user, payload):
        observed.append((user["id"], user_operation_lock(user["id"]).locked()))
        return {"ok": True}, None, 200

    monkeypatch.setattr(routes_module, "start_quiz", fake_operation)
    monkeypatch.setattr(routes_module, "get_current_question", fake_operation)
    monkeypatch.setattr(routes_module, "answer_quiz", fake_operation)

    for path in ("/api/quiz/start", "/api/quiz/current", "/api/quiz/answer"):
        response = http.post(path, json={}, headers=debug_headers(user_id))
        assert response.status_code == 200
        assert response.get_json() == {"ok": True}
        assert user_operation_lock(user_id).locked() is False

    assert observed == [(user_id, True), (user_id, True), (user_id, True)]
