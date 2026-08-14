import copy
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from types import SimpleNamespace

import pytest
from pymongo.errors import DuplicateKeyError, ServerSelectionTimeoutError

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
    def __init__(self, *, fail_writes=0):
        self.docs = {}
        self.fail_writes = fail_writes
        self.write_calls = 0

    def _maybe_fail(self):
        self.write_calls += 1
        if self.fail_writes:
            self.fail_writes -= 1
            raise ServerSelectionTimeoutError("temporary weekly leaderboard outage")

    def find_one(self, query):
        doc = self.docs.get(query["_id"])
        return copy.deepcopy(doc) if doc else None

    def insert_one(self, doc):
        self._maybe_fail()
        if doc["_id"] in self.docs:
            raise DuplicateKeyError("weekly best already exists")
        self.docs[doc["_id"]] = copy.deepcopy(doc)
        return SimpleNamespace(acknowledged=True, inserted_id=doc["_id"])

    def update_one(self, query, update, **kwargs):
        self._maybe_fail()
        doc = copy.deepcopy(self.docs.get(query["_id"], {"_id": query["_id"]}))
        doc.update(copy.deepcopy(update.get("$set", {})))
        self.docs[query["_id"]] = doc
        return SimpleNamespace(modified_count=1, acknowledged=True)


class BarrierWeeklyCollection(FakeWeeklyCollection):
    """Force two writers to observe the same absent weekly-best snapshot."""

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self._barrier = threading.Barrier(2)
        self._initial_reads = 0

    def find_one(self, query):
        wait = False
        with self._lock:
            if self._initial_reads < 2:
                self._initial_reads += 1
                wait = True
            doc = self.docs.get(query["_id"])
            result = copy.deepcopy(doc) if doc else None
        if wait:
            self._barrier.wait(timeout=2)
        return result

    def insert_one(self, doc):
        with self._lock:
            self.write_calls += 1
            if doc["_id"] in self.docs:
                raise DuplicateKeyError("weekly best already exists")
            self.docs[doc["_id"]] = copy.deepcopy(doc)
            return SimpleNamespace(acknowledged=True, inserted_id=doc["_id"])

    def update_one(self, query, update, **kwargs):
        with self._lock:
            current = self.docs.get(query["_id"])
            if current is None:
                return SimpleNamespace(modified_count=0, acknowledged=True)
            candidate = update["$set"]
            current_score = int(current.get("best_score", -1))
            current_time = float(current.get("best_time", float("inf")))
            candidate_score = int(candidate["best_score"])
            candidate_time = float(candidate["best_time"])
            if candidate_score < current_score or (
                candidate_score == current_score and candidate_time >= current_time
            ):
                return SimpleNamespace(modified_count=0, acknowledged=True)
            self.write_calls += 1
            current.update(copy.deepcopy(candidate))
            return SimpleNamespace(modified_count=1, acknowledged=True)


def _receipt():
    return {
        "points": 120,
        "daily_bonus": 100,
        "new_achievements": ["⭐ Perfect 20 — разблокировано!"],
        "kind": "challenge",
        "level_key": "random20",
        "week_id": "2026-W32",
        "applied_at": datetime.utcnow(),
    }


def test_iso_week_id_uses_iso_year_at_calendar_boundary():
    assert result_store._week_id_utc(datetime(2021, 1, 1, 12, 0, 0)) == "2020-W53"
    assert result_store._week_id_utc(datetime(2021, 1, 4, 12, 0, 0)) == "2021-W01"


def test_existing_challenge_receipt_retries_weekly_sync_after_transient_failure(monkeypatch):
    users = ReceiptUserCollection(_receipt())
    weekly = FakeWeeklyCollection(fail_writes=1)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)

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
    assert weekly.write_calls == 2
    stored = weekly.docs["2026-W32_random20_123"]
    assert stored["best_score"] == 20
    assert stored["best_time"] == 88.5


def test_recovery_after_week_boundary_uses_week_stored_in_receipt(monkeypatch):
    users = ReceiptUserCollection(_receipt())
    weekly = FakeWeeklyCollection()
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)
    monkeypatch.setattr(result_store, "_week_id_utc", lambda moment=None: "2026-W33")

    recovered = result_store.apply_challenge_result_once(
        user_id=123,
        result_id="session-weekly",
        username="tester",
        first_name="Test",
        mode="random20",
        score=20,
        total=20,
        time_seconds=88.5,
    )

    assert recovered["week_id"] == "2026-W32"
    assert "2026-W32_random20_123" in weekly.docs
    assert "2026-W33_random20_123" not in weekly.docs


def test_legacy_receipt_without_week_id_derives_week_from_applied_at(monkeypatch):
    receipt = _receipt()
    receipt.pop("week_id")
    receipt["applied_at"] = datetime(2021, 1, 1, 12, 0, 0)
    users = ReceiptUserCollection(receipt)
    weekly = FakeWeeklyCollection()
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)
    monkeypatch.setattr(result_store, "_week_id_utc", result_store._week_id_utc)

    result_store.apply_challenge_result_once(
        user_id=123,
        result_id="session-weekly",
        username="tester",
        first_name="Test",
        mode="random20",
        score=20,
        total=20,
        time_seconds=88.5,
    )

    assert "2020-W53_random20_123" in weekly.docs


def test_weekly_sync_keeps_better_existing_result(monkeypatch):
    weekly = FakeWeeklyCollection()
    weekly.docs["2026-W32_random20_123"] = {
        "_id": "2026-W32_random20_123",
        "best_score": 20,
        "best_time": 70.0,
    }
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)

    result_store._sync_weekly_challenge_result(
        user_id=123,
        username="tester",
        first_name="Test",
        mode="random20",
        score=20,
        time_seconds=88.5,
        week_id="2026-W32",
    )

    assert weekly.write_calls == 0
    assert weekly.docs["2026-W32_random20_123"]["best_time"] == 70.0


def test_weekly_sync_replaces_equal_score_with_faster_time(monkeypatch):
    weekly = FakeWeeklyCollection()
    weekly.docs["2026-W32_hardcore20_123"] = {
        "_id": "2026-W32_hardcore20_123",
        "best_score": 18,
        "best_time": 95.0,
    }
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)

    result_store._sync_weekly_challenge_result(
        user_id=123,
        username="tester",
        first_name="Test",
        mode="hardcore20",
        score=18,
        time_seconds=90.0,
        week_id="2026-W32",
    )

    assert weekly.write_calls == 1
    assert weekly.docs["2026-W32_hardcore20_123"]["best_time"] == 90.0


def test_concurrent_weekly_sync_cannot_regress_a_better_result(monkeypatch):
    weekly = BarrierWeeklyCollection()
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)

    def write(score, elapsed):
        result_store._sync_weekly_challenge_result(
            user_id=123,
            username="tester",
            first_name="Test",
            mode="random20",
            score=score,
            time_seconds=elapsed,
            week_id="2026-W32",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        better = pool.submit(write, 20, 80.0)
        worse = pool.submit(write, 18, 60.0)
        better.result(timeout=3)
        worse.result(timeout=3)

    stored = weekly.docs["2026-W32_random20_123"]
    assert stored["best_score"] == 20
    assert stored["best_time"] == 80.0
