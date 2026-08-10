from copy import deepcopy

import pytest
from pymongo.errors import PyMongoError

import database
from legacy_session_retention import (
    QuizSessionRetentionUnavailable,
    ensure_state_aware_session_ttl,
)


class FakeIndexes:
    def __init__(self, info=None, *, fail=False):
        self.info = deepcopy(info or {})
        self.fail = fail
        self.dropped = []
        self.created = []

    def index_information(self):
        if self.fail:
            raise PyMongoError("index lookup failed")
        return deepcopy(self.info)

    def drop_index(self, name):
        self.dropped.append(name)
        self.info.pop(name, None)

    def create_index(self, keys, **kwargs):
        self.created.append((deepcopy(keys), deepcopy(kwargs)))
        self.info[kwargs["name"]] = {
            "key": list(keys),
            "expireAfterSeconds": kwargs.get("expireAfterSeconds"),
            "partialFilterExpression": deepcopy(kwargs.get("partialFilterExpression")),
        }
        return kwargs["name"]


def test_migration_drops_generic_ttl_and_creates_terminal_only_ttl(monkeypatch):
    collection = FakeIndexes(
        {
            "_id_": {"key": [("_id", 1)]},
            "ttl_updated_at": {
                "key": [("updated_at_dt", 1)],
                "expireAfterSeconds": 21600,
            },
        }
    )
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert ensure_state_aware_session_ttl() is True

    assert collection.dropped == ["ttl_updated_at"]
    assert collection.created == [
        (
            [("updated_at_dt", 1)],
            {
                "expireAfterSeconds": 7776000,
                "partialFilterExpression": {
                    "status": {"$in": ["finished", "cancelled"]}
                },
                "name": "ttl_terminal_updated_at",
                "background": True,
            },
        )
    ]


def test_matching_terminal_ttl_is_idempotent(monkeypatch):
    collection = FakeIndexes(
        {
            "ttl_terminal_updated_at": {
                "key": [("updated_at_dt", 1)],
                "expireAfterSeconds": 7776000,
                "partialFilterExpression": {
                    "status": {"$in": ["finished", "cancelled"]}
                },
            }
        }
    )
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert ensure_state_aware_session_ttl() is True
    assert collection.dropped == []
    assert collection.created == []


def test_wrong_terminal_options_are_replaced(monkeypatch):
    collection = FakeIndexes(
        {
            "ttl_terminal_updated_at": {
                "key": [("updated_at_dt", 1)],
                "expireAfterSeconds": 3600,
                "partialFilterExpression": {"status": "finished"},
            }
        }
    )
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert ensure_state_aware_session_ttl() is True
    assert collection.dropped == ["ttl_terminal_updated_at"]
    assert len(collection.created) == 1


def test_missing_collection_is_explicit_noop(monkeypatch):
    monkeypatch.setattr(database, "quiz_sessions_collection", None)

    assert ensure_state_aware_session_ttl() is False


def test_index_failure_is_not_silently_treated_as_safe(monkeypatch):
    monkeypatch.setattr(database, "quiz_sessions_collection", FakeIndexes(fail=True))

    with pytest.raises(QuizSessionRetentionUnavailable, match="retention migration failed"):
        ensure_state_aware_session_ttl()
