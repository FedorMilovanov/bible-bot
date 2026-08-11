from copy import deepcopy
from types import SimpleNamespace

import pytest

import web_api.db_hardening as hardening


class FakeSessions:
    def __init__(self, duplicate_groups=None, indexes=None):
        self.duplicate_groups = list(duplicate_groups or [])
        self.indexes = deepcopy(indexes or {"_id_": {"key": [("_id", 1)]}})
        self.events = []

    def index_information(self):
        self.events.append(("index_information", None))
        return deepcopy(self.indexes)

    def aggregate(self, pipeline):
        self.events.append(("aggregate", deepcopy(pipeline)))
        return iter(deepcopy(self.duplicate_groups))

    def drop_index(self, name):
        self.events.append(("drop_index", name))
        self.indexes.pop(name, None)

    def create_index(self, keys, **kwargs):
        name = kwargs.get("name", "idx")
        self.events.append(("create_index", name))
        metadata = {"key": [tuple(item) for item in keys]}
        if "expireAfterSeconds" in kwargs:
            metadata["expireAfterSeconds"] = kwargs["expireAfterSeconds"]
        if "partialFilterExpression" in kwargs:
            metadata["partialFilterExpression"] = deepcopy(kwargs["partialFilterExpression"])
        if "unique" in kwargs:
            metadata["unique"] = kwargs["unique"]
        self.indexes[name] = metadata
        return name


class FakeDB:
    def __init__(self, sessions):
        self.sessions = sessions

    def __getitem__(self, name):
        assert name == "miniapp_sessions"
        return self.sessions


def test_duplicate_preflight_reports_all_open_sessions_without_mutation():
    sessions = FakeSessions(
        [
            {
                "_id": "user-1",
                "session_ids": ["session-new", "session-old-2", "session-old-1"],
                "count": 3,
            }
        ]
    )

    duplicates = hardening.find_duplicate_active_users(sessions)

    assert duplicates == [{
        "_id": "user-1",
        "session_ids": ["session-new", "session-old-2", "session-old-1"],
        "count": 3,
    }]
    assert [event for event, _payload in sessions.events] == ["aggregate"]
    # Deliberately no update_many method: duplicate preflight must remain read-only.


def test_duplicate_preflight_pipeline_never_selects_a_winner():
    sessions = FakeSessions()

    assert hardening.find_duplicate_active_users(sessions) == []

    pipeline = sessions.events[0][1]
    assert pipeline[0] == {
        "$match": {"status": {"$in": list(hardening.OPEN_STATUSES)}}
    }
    assert pipeline[1] == {
        "$group": {
            "_id": "$user_id",
            "session_ids": {"$push": "$_id"},
            "count": {"$sum": 1},
        }
    }
    assert pipeline[2] == {"$match": {"count": {"$gt": 1}}}
    assert pipeline[3] == {"$sort": {"_id": 1}}
    assert pipeline[4] == {"$limit": 20}


def test_unique_index_is_created_only_after_duplicate_preflight(monkeypatch):
    sessions = FakeSessions()
    fake_database = SimpleNamespace(db=FakeDB(sessions))
    monkeypatch.setitem(__import__("sys").modules, "database", fake_database)
    monkeypatch.setattr(hardening, "_INDEXES_READY", False)

    assert hardening.ensure_miniapp_indexes() is True

    event_names = [event for event, _payload in sessions.events]
    unique_position = sessions.events.index(("create_index", hardening.UNIQUE_ACTIVE_NAME))
    preflight_position = event_names.index("aggregate")
    assert preflight_position < unique_position
    assert hardening._INDEXES_READY is True

    terminal = sessions.indexes[hardening.TERMINAL_TTL_NAME]
    assert terminal["expireAfterSeconds"] == hardening.TERMINAL_RETENTION_SECONDS
    assert terminal["partialFilterExpression"] == hardening.TERMINAL_FILTER
    unique = sessions.indexes[hardening.UNIQUE_ACTIVE_NAME]
    assert unique["unique"] is True
    assert unique["partialFilterExpression"] == hardening.OPEN_FILTER


def test_duplicate_open_sessions_block_unique_index_without_auto_repair(monkeypatch):
    sessions = FakeSessions([
        {"_id": "user-1", "session_ids": ["a", "b"], "count": 2},
    ])
    fake_database = SimpleNamespace(db=FakeDB(sessions))
    monkeypatch.setitem(__import__("sys").modules, "database", fake_database)
    monkeypatch.setattr(hardening, "_INDEXES_READY", False)

    with pytest.raises(
        hardening.MiniAppIndexSafetyUnavailable,
        match="operator review",
    ):
        hardening.ensure_miniapp_indexes()

    assert hardening.UNIQUE_ACTIVE_NAME not in sessions.indexes
    assert not any(
        event == "create_index" and payload == hardening.UNIQUE_ACTIVE_NAME
        for event, payload in sessions.events
    )
