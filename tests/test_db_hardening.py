from types import SimpleNamespace

import web_api.db_hardening as hardening


class FakeSessions:
    def __init__(self, duplicate_groups=None):
        self.duplicate_groups = list(duplicate_groups or [])
        self.events = []
        self.update_calls = []

    def aggregate(self, pipeline):
        self.events.append(("aggregate", pipeline))
        return iter(self.duplicate_groups)

    def update_many(self, query, update):
        self.events.append(("update_many", query))
        self.update_calls.append((query, update))
        return SimpleNamespace(modified_count=len(query.get("_id", {}).get("$in", [])))

    def create_index(self, keys, **kwargs):
        self.events.append(("create_index", kwargs.get("name")))
        return kwargs.get("name")


class FakeDB:
    def __init__(self, sessions):
        self.sessions = sessions

    def __getitem__(self, name):
        assert name == "miniapp_sessions"
        return self.sessions


def test_duplicate_repair_keeps_newest_and_only_abandons_still_active():
    sessions = FakeSessions(
        [
            {
                "_id": "user-1",
                "keep": "session-new",
                "ids": ["session-new", "session-old-2", "session-old-1"],
                "count": 3,
            }
        ]
    )

    repaired = hardening._repair_duplicate_active_sessions(sessions)

    assert repaired == 2
    assert len(sessions.update_calls) == 1
    query, update = sessions.update_calls[0]
    assert query == {
        "_id": {"$in": ["session-old-2", "session-old-1"]},
        "status": "in_progress",
    }
    assert update["$set"]["status"] == "abandoned"
    assert update["$set"]["abandon_reason"] == "duplicate_active_repair"
    assert "updated_at_dt" in update["$set"]


def test_duplicate_repair_pipeline_prefers_most_recent_session():
    sessions = FakeSessions()

    assert hardening._repair_duplicate_active_sessions(sessions) == 0

    pipeline = sessions.events[0][1]
    assert pipeline[0] == {"$match": {"status": "in_progress"}}
    assert pipeline[1] == {"$sort": {"updated_at_dt": -1, "_id": -1}}
    assert pipeline[-1] == {"$match": {"count": {"$gt": 1}}}


def test_unique_index_is_created_only_after_legacy_duplicate_repair(monkeypatch):
    sessions = FakeSessions()
    fake_database = SimpleNamespace(db=FakeDB(sessions))
    monkeypatch.setitem(__import__("sys").modules, "database", fake_database)
    monkeypatch.setattr(hardening, "_INDEXES_READY", False)

    assert hardening.ensure_miniapp_indexes() is True

    event_names = [event for event, _payload in sessions.events]
    unique_position = sessions.events.index(("create_index", "uniq_miniapp_active_user"))
    repair_position = event_names.index("aggregate")
    assert repair_position < unique_position
    assert hardening._INDEXES_READY is True
