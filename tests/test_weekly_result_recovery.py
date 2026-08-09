import copy
from datetime import datetime
from types import SimpleNamespace

import pytest
from pymongo.errors import ServerSelectionTimeoutError

import database
from web_api import result_store


class ReceiptUserCollection:
    def __init__(self, receipt):
        self.doc = {
            "_id": "123",
            "miniapp_result_receipts": {"session-weekly": copy.deepcopy(receipt)},
        }

    def find_one(self, query, *args, **kwargs):
        return copy.deepcopy(self.doc) if query.get("_id") == "123" else None


class FakeWeeklyCollection:
    def __init__(self, *, fail_updates=0):
        self.docs = {}
        self.fail_updates = fail_updates
        self.update_calls = 0

    def find_one(self, query):
        doc = self.docs.get(query["_id"])
        return copy.deepcopy(doc) if doc else None

    def update_one(self, query, update, **kwargs):
        self.update_calls += 1
        if self.fail_updates:
            self.fail_updates -= 1
            raise ServerSelectionTimeoutError("temporary weekly leaderboard outage")
        doc = copy.deepcopy(self.docs.get(query["_id"], {"_id": query["_id"]}))
        doc.update(copy.deepcopy(update.get("$set", {})))
        self.docs[query["_id"]] = doc
        return SimpleNamespace(modified_count=1)


def _receipt():
    return {
        "points": 120,
        "daily_bonus": 100,
        "new_achievements": ["⭐ Perfect 20 — разблокировано!"],
        "kind": "challenge",
        "level_key": "random20",
        "applied_at": datetime.utcnow(),
    }


def test_existing_challenge_receipt_retries_weekly_sync_after_transient_failure(monkeypatch):
    users = ReceiptUserCollection(_receipt())
    weekly = FakeWeeklyCollection(fail_updates=1)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)
    monkeypatch.setattr(database, "get_current_week_id", lambda: "2026-W32")

    kwargs = {
        "user_id": 123,
        "result_id": "session-weekly",
        "username": "tester",
        "first_name": "Test",
        "mode": "random20",
        "score": 20,
        "total": 20,
        "time_seconds": 88.5,
    }

    with pytest.raises(ServerSelectionTimeoutError):
        result_store.apply_challenge_result_once(**kwargs)

    recovered = result_store.apply_challenge_result_once(**kwargs)

    assert recovered["points"] == 120
    assert weekly.update_calls == 2
    stored = weekly.docs["2026-W32_random20_123"]
    assert stored["best_score"] == 20
    assert stored["best_time"] == 88.5


def test_weekly_sync_keeps_better_existing_result(monkeypatch):
    weekly = FakeWeeklyCollection()
    weekly.docs["2026-W32_random20_123"] = {
        "_id": "2026-W32_random20_123",
        "best_score": 20,
        "best_time": 70.0,
    }
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)
    monkeypatch.setattr(database, "get_current_week_id", lambda: "2026-W32")

    result_store._sync_weekly_challenge_result(
        user_id=123,
        username="tester",
        first_name="Test",
        mode="random20",
        score=20,
        time_seconds=88.5,
    )

    assert weekly.update_calls == 0
    assert weekly.docs["2026-W32_random20_123"]["best_time"] == 70.0


def test_weekly_sync_replaces_equal_score_with_faster_time(monkeypatch):
    weekly = FakeWeeklyCollection()
    weekly.docs["2026-W32_hardcore20_123"] = {
        "_id": "2026-W32_hardcore20_123",
        "best_score": 18,
        "best_time": 95.0,
    }
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)
    monkeypatch.setattr(database, "get_current_week_id", lambda: "2026-W32")

    result_store._sync_weekly_challenge_result(
        user_id=123,
        username="tester",
        first_name="Test",
        mode="hardcore20",
        score=18,
        time_seconds=90.0,
    )

    assert weekly.update_calls == 1
    assert weekly.docs["2026-W32_hardcore20_123"]["best_time"] == 90.0
