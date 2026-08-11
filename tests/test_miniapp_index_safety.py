from copy import deepcopy

import pytest

import database
import web_api.db_hardening as hardening
import web_api.quiz as quiz


class FakeSessions:
    def __init__(self, *, indexes=None, duplicates=None, info_error=None):
        self.indexes = deepcopy(indexes or {"_id_": {"key": [("_id", 1)]}})
        self.duplicates = deepcopy(duplicates or [])
        self.info_error = info_error
        self.created = []
        self.dropped = []
        self.aggregate_calls = []

    def index_information(self):
        if self.info_error is not None:
            raise self.info_error
        return deepcopy(self.indexes)

    def drop_index(self, name):
        self.dropped.append(name)
        self.indexes.pop(name, None)

    def create_index(self, key, **kwargs):
        name = kwargs["name"]
        self.created.append((deepcopy(key), deepcopy(kwargs)))
        info = {"key": [tuple(item) for item in key]}
        if "expireAfterSeconds" in kwargs:
            info["expireAfterSeconds"] = kwargs["expireAfterSeconds"]
        if "partialFilterExpression" in kwargs:
            info["partialFilterExpression"] = deepcopy(kwargs["partialFilterExpression"])
        if "unique" in kwargs:
            info["unique"] = kwargs["unique"]
        self.indexes[name] = info
        return name

    def aggregate(self, pipeline):
        self.aggregate_calls.append(deepcopy(pipeline))
        return deepcopy(self.duplicates)


class FakeDb:
    def __init__(self, sessions):
        self.sessions = sessions

    def __getitem__(self, name):
        assert name == "miniapp_sessions"
        return self.sessions


def install(monkeypatch, sessions):
    monkeypatch.setattr(database, "db", FakeDb(sessions), raising=False)
    monkeypatch.setattr(hardening, "_INDEXES_READY", False)


def test_legacy_generic_ttl_is_replaced_by_terminal_only_retention(monkeypatch):
    sessions = FakeSessions(indexes={
        "_id_": {"key": [("_id", 1)]},
        hardening.LEGACY_TTL_NAME: {
            "key": [("updated_at_dt", 1)],
            "expireAfterSeconds": 6 * 60 * 60,
        },
    })
    install(monkeypatch, sessions)

    assert hardening.ensure_miniapp_indexes() is True

    assert hardening.LEGACY_TTL_NAME in sessions.dropped
    assert hardening.LEGACY_TTL_NAME not in sessions.indexes
    terminal = sessions.indexes[hardening.TERMINAL_TTL_NAME]
    assert terminal["key"] == [("updated_at_dt", 1)]
    assert terminal["expireAfterSeconds"] == hardening.TERMINAL_RETENTION_SECONDS
    assert terminal["partialFilterExpression"] == hardening.TERMINAL_FILTER
    assert "in_progress" not in hardening.TERMINAL_FILTER["status"]["$in"]
    assert "finalizing" not in hardening.TERMINAL_FILTER["status"]["$in"]
    assert "score_error" not in hardening.TERMINAL_FILTER["status"]["$in"]


def test_duplicate_open_sessions_block_uniqueness_without_mutating_rows(monkeypatch):
    sessions = FakeSessions(duplicates=[{
        "_id": "101",
        "session_ids": ["old", "new"],
        "count": 2,
    }])
    install(monkeypatch, sessions)

    with pytest.raises(
        hardening.MiniAppIndexSafetyUnavailable,
        match="operator review",
    ):
        hardening.ensure_miniapp_indexes()

    assert hardening.UNIQUE_ACTIVE_NAME not in sessions.indexes
    assert sessions.aggregate_calls
    assert sessions.aggregate_calls[0][0] == {
        "$match": {"status": {"$in": list(hardening.OPEN_STATUSES)}}
    }
    # The fake intentionally has no update_many method. Reintroducing automatic
    # duplicate abandonment would fail instead of silently choosing a winner.


def test_incompatible_terminal_index_is_dropped_and_recreated(monkeypatch):
    sessions = FakeSessions(indexes={
        "_id_": {"key": [("_id", 1)]},
        hardening.TERMINAL_TTL_NAME: {
            "key": [("created_at_dt", 1)],
            "expireAfterSeconds": hardening.TERMINAL_RETENTION_SECONDS,
            "partialFilterExpression": deepcopy(hardening.TERMINAL_FILTER),
        },
    })
    install(monkeypatch, sessions)

    assert hardening.ensure_miniapp_indexes() is True

    assert hardening.TERMINAL_TTL_NAME in sessions.dropped
    assert sessions.indexes[hardening.TERMINAL_TTL_NAME]["key"] == [("updated_at_dt", 1)]


def test_unique_open_index_is_exact_and_partial(monkeypatch):
    sessions = FakeSessions()
    install(monkeypatch, sessions)

    assert hardening.ensure_miniapp_indexes() is True

    unique = sessions.indexes[hardening.UNIQUE_ACTIVE_NAME]
    assert unique["key"] == [("user_id", 1)]
    assert unique["unique"] is True
    assert unique["partialFilterExpression"] == hardening.OPEN_FILTER
    assert set(unique["partialFilterExpression"]["status"]["$in"]) == {
        "in_progress",
        "finalizing",
        "score_error",
    }


def test_old_in_progress_only_unique_index_is_preserved_for_operator_migration(monkeypatch):
    old_unique = {
        "key": [("user_id", 1)],
        "unique": True,
        "partialFilterExpression": {"status": "in_progress"},
    }
    sessions = FakeSessions(indexes={
        "_id_": {"key": [("_id", 1)]},
        hardening.UNIQUE_ACTIVE_NAME: deepcopy(old_unique),
    })
    install(monkeypatch, sessions)

    with pytest.raises(
        hardening.MiniAppIndexSafetyUnavailable,
        match="operator migration",
    ):
        hardening.ensure_miniapp_indexes()

    assert hardening.UNIQUE_ACTIVE_NAME not in sessions.dropped
    assert sessions.indexes[hardening.UNIQUE_ACTIVE_NAME] == old_unique
    assert all(
        kwargs.get("name") != hardening.UNIQUE_ACTIVE_NAME
        for _key, kwargs in sessions.created
    )


def test_index_metadata_failure_is_explicit(monkeypatch):
    sessions = FakeSessions(info_error=RuntimeError("mongo unavailable"))
    install(monkeypatch, sessions)

    with pytest.raises(
        hardening.MiniAppIndexSafetyUnavailable,
        match="hardening failed",
    ):
        hardening.ensure_miniapp_indexes()


def test_quiz_collection_is_not_exposed_when_index_safety_fails(monkeypatch):
    sessions = FakeSessions()
    monkeypatch.setattr(database, "db", FakeDb(sessions), raising=False)

    def unsafe():
        raise hardening.MiniAppIndexSafetyUnavailable("unsafe")

    monkeypatch.setattr(quiz, "ensure_miniapp_indexes", unsafe)

    assert quiz.miniapp_sessions() is None


def test_missing_database_keeps_existing_false_contract(monkeypatch):
    monkeypatch.setattr(database, "db", None, raising=False)
    monkeypatch.setattr(hardening, "_INDEXES_READY", False)

    assert hardening.ensure_miniapp_indexes() is False
