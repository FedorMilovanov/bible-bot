from datetime import datetime
from types import SimpleNamespace

import pytest
from pymongo.errors import AutoReconnect, DuplicateKeyError

import database
import legacy_session_access as access


class FakeSessions:
    def __init__(self):
        self.indexes = {}
        self.created_indexes = []
        self.inserted = []
        self.active = None
        self.insert_error = None
        self.find_error = None

    def index_information(self):
        return self.indexes

    def create_index(self, keys, **kwargs):
        self.created_indexes.append((list(keys), dict(kwargs)))
        if kwargs.get("name") == access._ACTIVE_INDEX:
            self.indexes[access._ACTIVE_INDEX] = {
                "key": list(keys),
                "unique": kwargs.get("unique"),
                "partialFilterExpression": kwargs.get("partialFilterExpression"),
            }
        return kwargs.get("name")

    def insert_one(self, doc):
        if self.insert_error is not None:
            raise self.insert_error
        self.inserted.append(doc)
        return SimpleNamespace(inserted_id=doc["_id"])

    def find_one(self, query):
        if self.find_error is not None:
            raise self.find_error
        if self.active is None:
            return None
        for key, value in query.items():
            if self.active.get(key) != value:
                return None
        return self.active


def _install(monkeypatch):
    collection = FakeSessions()
    now = datetime(2026, 8, 10, 12, 0, 0)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: now)
    monkeypatch.setattr(access.time, "time", lambda: 123.5)
    return collection, now


def _create(**overrides):
    kwargs = {
        "user_id": 42,
        "mode": "level",
        "question_ids": ["q1", "q2"],
        "questions_data": [{"question": "Q1"}, {"question": "Q2"}],
        "level_key": "easy",
        "level_name": "Easy",
        "time_limit": 30,
        "chat_id": 100,
    }
    kwargs.update(overrides)
    return access.create_quiz_session_strict(**kwargs)


def test_create_is_durable_and_installs_one_active_session_index(monkeypatch):
    collection, now = _install(monkeypatch)

    doc = _create()

    assert doc["user_id"] == "42"
    assert doc["status"] == "in_progress"
    assert doc["current_index"] == 0
    assert doc["correct_count"] == 0
    assert doc["start_time"] == 123.5
    assert doc["created_at"] == now
    assert collection.inserted == [doc]
    assert collection.created_indexes == [
        (
            [("user_id", 1)],
            {
                "unique": True,
                "partialFilterExpression": {"status": "in_progress"},
                "name": "uniq_active_quiz_user",
                "background": True,
            },
        )
    ]


def test_matching_unique_index_is_idempotent(monkeypatch):
    collection, _now = _install(monkeypatch)
    collection.indexes[access._ACTIVE_INDEX] = {
        "key": [("user_id", 1)],
        "unique": True,
        "partialFilterExpression": {"status": "in_progress"},
    }

    assert access.ensure_active_session_unique_index() is True
    assert collection.created_indexes == []


def test_incompatible_named_index_fails_closed(monkeypatch):
    collection, _now = _install(monkeypatch)
    collection.indexes[access._ACTIVE_INDEX] = {
        "key": [("user_id", 1)],
        "unique": False,
        "partialFilterExpression": {"status": "in_progress"},
    }

    with pytest.raises(access.QuizSessionAccessSchemaInvalid, match="incompatible"):
        access.ensure_active_session_unique_index()


def test_duplicate_insert_confirms_existing_active_session(monkeypatch):
    collection, _now = _install(monkeypatch)
    collection.insert_error = DuplicateKeyError("duplicate")
    collection.active = {
        "_id": "existing",
        "user_id": "42",
        "status": "in_progress",
    }

    with pytest.raises(access.QuizSessionAlreadyActive, match="already has"):
        _create()


def test_unexplained_duplicate_is_schema_error(monkeypatch):
    collection, _now = _install(monkeypatch)
    collection.insert_error = DuplicateKeyError("duplicate")

    with pytest.raises(access.QuizSessionAccessSchemaInvalid, match="unexplained"):
        _create()


def test_mongo_insert_outage_never_returns_phantom_session(monkeypatch):
    collection, _now = _install(monkeypatch)
    collection.insert_error = AutoReconnect("mongo unavailable")

    with pytest.raises(access.QuizSessionAccessUnavailable, match="creation failed"):
        _create()

    assert collection.inserted == []


def test_active_lookup_distinguishes_absent_from_outage(monkeypatch):
    collection, _now = _install(monkeypatch)

    assert access.get_active_quiz_session_strict(42) is None

    collection.find_error = AutoReconnect("mongo unavailable")
    with pytest.raises(access.QuizSessionAccessUnavailable, match="lookup failed"):
        access.get_active_quiz_session_strict(42)


def test_owner_scoped_session_lookup(monkeypatch):
    collection, _now = _install(monkeypatch)
    collection.active = {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
    }

    assert access.get_quiz_session_strict("s1", user_id=42) == collection.active
    assert access.get_quiz_session_strict("s1", user_id=99) is None


def test_invalid_session_payload_is_rejected_before_insert(monkeypatch):
    collection, _now = _install(monkeypatch)

    with pytest.raises(ValueError, match="unsupported"):
        _create(mode="other")
    with pytest.raises(ValueError, match="length mismatch"):
        _create(question_ids=["q1"])
    with pytest.raises(ValueError, match="time_limit"):
        _create(time_limit=0)

    assert collection.inserted == []
    assert collection.created_indexes == []
