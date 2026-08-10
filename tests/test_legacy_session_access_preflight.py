import pytest
from pymongo.errors import AutoReconnect

import database
from legacy_session_access import (
    QuizSessionAccessUnavailable,
    find_duplicate_active_session_users,
)


class AggregateSessions:
    def __init__(self, rows=None, error=None):
        self.rows = rows or []
        self.error = error
        self.pipeline = None

    def aggregate(self, pipeline):
        self.pipeline = pipeline
        if self.error is not None:
            raise self.error
        return list(self.rows)


def test_duplicate_preflight_is_read_only_and_bounded(monkeypatch):
    collection = AggregateSessions([
        {"_id": "42", "count": 3},
        {"_id": "99", "count": 2},
    ])
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    rows = find_duplicate_active_session_users(limit=10)

    assert rows == [
        {"user_id": "42", "count": 3},
        {"user_id": "99", "count": 2},
    ]
    assert collection.pipeline[-1] == {"$limit": 10}
    assert collection.pipeline[0] == {"$match": {"status": "in_progress"}}


def test_duplicate_preflight_outage_is_explicit(monkeypatch):
    monkeypatch.setattr(
        database,
        "quiz_sessions_collection",
        AggregateSessions(error=AutoReconnect("mongo unavailable")),
    )

    with pytest.raises(QuizSessionAccessUnavailable, match="preflight failed"):
        find_duplicate_active_session_users()


def test_duplicate_preflight_validates_limit_before_mongo(monkeypatch):
    collection = AggregateSessions()
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    with pytest.raises(ValueError, match="positive integer"):
        find_duplicate_active_session_users(limit=0)
    assert collection.pipeline is None
