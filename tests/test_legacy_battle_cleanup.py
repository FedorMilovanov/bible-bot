from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from pymongo.errors import PyMongoError

import legacy_battle_cleanup as cleanup


class FakeCollection:
    def __init__(self, *, deleted_count=0, error=None):
        self.deleted_count = deleted_count
        self.error = error
        self.calls = []

    def delete_many(self, query):
        self.calls.append(query)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(deleted_count=self.deleted_count)


def install(monkeypatch, collection, now):
    monkeypatch.setattr(cleanup, "_battle_collection", lambda: collection)
    monkeypatch.setattr(cleanup, "_database", lambda: SimpleNamespace(_now_utc=lambda: now))


def test_cleanup_expires_waiting_from_creation_and_joined_from_join_time(monkeypatch):
    now = datetime(2026, 8, 11, 12, 0, 0)
    cutoff = now - timedelta(minutes=10)
    collection = FakeCollection(deleted_count=3)
    install(monkeypatch, collection, now)

    deleted = cleanup.cleanup_stale_waiting_battles(max_age_minutes=10)

    assert deleted == 3
    assert collection.calls == [{
        "$or": [
            {
                "status": "waiting",
                "created_at_dt": {"$lt": cutoff},
            },
            {
                "status": "in_progress",
                "joined_at_dt": {"$lt": cutoff},
            },
            {
                "status": "in_progress",
                "joined_at_dt": None,
                "created_at_dt": {"$lt": cutoff},
            },
        ],
        "creator_finished": {"$ne": True},
        "opponent_finished": {"$ne": True},
        "final_claimed": {"$ne": True},
        "live_progress": {"$exists": False},
    }]


def test_cleanup_keeps_recovery_evidence_guards_at_top_level(monkeypatch):
    now = datetime(2026, 8, 11, 12, 0, 0)
    collection = FakeCollection()
    install(monkeypatch, collection, now)

    cleanup.cleanup_stale_waiting_battles()

    query = collection.calls[0]
    assert query["creator_finished"] == {"$ne": True}
    assert query["opponent_finished"] == {"$ne": True}
    assert query["final_claimed"] == {"$ne": True}
    assert query["live_progress"] == {"$exists": False}


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "10"])
def test_cleanup_rejects_invalid_age_before_mongo(monkeypatch, value):
    collection = FakeCollection()
    install(monkeypatch, collection, datetime(2026, 8, 11, 12, 0, 0))

    with pytest.raises(ValueError, match="positive integer"):
        cleanup.cleanup_stale_waiting_battles(max_age_minutes=value)
    assert collection.calls == []


def test_cleanup_storage_failure_is_explicit(monkeypatch):
    collection = FakeCollection(error=PyMongoError("mongo unavailable"))
    install(monkeypatch, collection, datetime(2026, 8, 11, 12, 0, 0))

    with pytest.raises(cleanup.LegacyBattleCleanupUnavailable, match="cleanup failed"):
        cleanup.cleanup_stale_waiting_battles()


def test_cleanup_rejects_malformed_delete_result(monkeypatch):
    now = datetime(2026, 8, 11, 12, 0, 0)
    collection = FakeCollection(deleted_count=True)
    install(monkeypatch, collection, now)

    with pytest.raises(cleanup.LegacyBattleCleanupUnavailable, match="result is invalid"):
        cleanup.cleanup_stale_waiting_battles()
