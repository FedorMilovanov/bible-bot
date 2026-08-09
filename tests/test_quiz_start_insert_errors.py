import pytest
from pymongo.errors import DuplicateKeyError, ServerSelectionTimeoutError

import database
from web_api import quiz


class FailingInsertSessions:
    def __init__(self, error):
        self.error = error

    def update_many(self, *_args, **_kwargs):
        return object()

    def insert_one(self, _document):
        raise self.error


def _start_with_insert_error(monkeypatch, error):
    sessions = FailingInsertSessions(error)
    monkeypatch.setattr(quiz, "miniapp_sessions", lambda: sessions)
    monkeypatch.setattr(database, "init_user_stats", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(database, "get_user_stats", lambda user_id: {"_id": str(user_id)})

    return quiz.start_quiz(
        {"id": 991401, "username": "tester", "first_name": "Test"},
        {"pool_key": "easy_p1", "mode": "relaxed", "count": 10},
    )


def test_duplicate_active_session_insert_is_conflict(monkeypatch):
    body, message, status = _start_with_insert_error(monkeypatch, DuplicateKeyError("duplicate"))

    assert body is None
    assert status == 409
    assert message == "another active quiz already exists; retry start"


def test_mongo_insert_outage_is_retryable_service_error(monkeypatch):
    body, message, status = _start_with_insert_error(
        monkeypatch,
        ServerSelectionTimeoutError("temporary database outage"),
    )

    assert body is None
    assert status == 503
    assert message == "database temporarily unavailable"


def test_unexpected_insert_failure_is_internal_error(monkeypatch):
    body, message, status = _start_with_insert_error(monkeypatch, RuntimeError("unexpected"))

    assert body is None
    assert status == 500
    assert message == "could not create quiz session"
