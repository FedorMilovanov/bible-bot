from types import SimpleNamespace

from pymongo.errors import DuplicateKeyError, ServerSelectionTimeoutError

import database
from web_api import quiz


class FailingInsertSessions:
    def __init__(self, error=None, *, acknowledged=True):
        self.error = error
        self.acknowledged = acknowledged

    def find_one(self, _query):
        return None

    def insert_one(self, _document):
        if self.error is not None:
            raise self.error
        return SimpleNamespace(acknowledged=self.acknowledged)


def _start_with_sessions(monkeypatch, sessions):
    monkeypatch.setattr(quiz, "miniapp_sessions", lambda: sessions)
    monkeypatch.setattr(database, "init_user_stats", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(database, "get_user_stats", lambda user_id: {"_id": str(user_id)})

    return quiz.start_quiz(
        {"id": 991401, "username": "tester", "first_name": "Test"},
        {"pool_key": "easy_p1", "mode": "relaxed", "count": 10},
    )


def _start_with_insert_error(monkeypatch, error):
    return _start_with_sessions(monkeypatch, FailingInsertSessions(error))


def test_duplicate_open_session_insert_is_conflict(monkeypatch):
    body, message, status = _start_with_insert_error(monkeypatch, DuplicateKeyError("duplicate"))

    assert body is None
    assert status == 409
    assert message == "another unfinished quiz already exists; retry start"


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


def test_unacknowledged_insert_cannot_return_a_fake_session_id(monkeypatch):
    body, message, status = _start_with_sessions(
        monkeypatch,
        FailingInsertSessions(acknowledged=False),
    )

    assert body is None
    assert status == 503
    assert message == "database did not acknowledge quiz session creation"
