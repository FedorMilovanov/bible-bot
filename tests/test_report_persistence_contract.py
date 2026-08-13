from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace

import database


class FakeUsers:
    def __init__(self, *, entry=None, find_error=None, update_error=None, modified_count=1):
        self.entry = deepcopy(entry)
        self.find_error = find_error
        self.update_error = update_error
        self.modified_count = modified_count
        self.update_calls = []

    def find_one(self, query, projection=None):
        if self.find_error:
            raise self.find_error
        return deepcopy(self.entry)

    def update_one(self, query, update):
        self.update_calls.append((deepcopy(query), deepcopy(update)))
        if self.update_error:
            raise self.update_error
        return SimpleNamespace(modified_count=self.modified_count)


class FakeReports:
    def __init__(
        self,
        *,
        insert_error=None,
        update_error=None,
        modified_count=1,
        existing=None,
    ):
        self.insert_error = insert_error
        self.update_error = update_error
        self.modified_count = modified_count
        self.existing = deepcopy(existing)
        self.inserted = []
        self.update_calls = []

    def insert_one(self, doc):
        if self.insert_error:
            raise self.insert_error
        self.inserted.append(deepcopy(doc))
        return SimpleNamespace(inserted_id=doc["_id"])

    def update_one(self, query, update):
        self.update_calls.append((deepcopy(query), deepcopy(update)))
        if self.update_error:
            raise self.update_error
        return SimpleNamespace(modified_count=self.modified_count)

    def find_one(self, query, projection=None):
        return deepcopy(self.existing)


def _now():
    return datetime(2026, 8, 10, 12, 0, 0)


def test_report_submission_fails_closed_when_storage_is_missing(monkeypatch):
    monkeypatch.setattr(database, "collection", None)
    monkeypatch.setattr(database, "reports_collection", None)

    assert database.can_submit_report(42) is False
    assert database.seconds_until_next_report(42) == database.REPORT_COOLDOWN_SECONDS
    assert database.insert_report(42, "u", "User", "bug", "text") is None
    assert database.mark_report_delivered("r1") is False


def test_report_cooldown_read_error_fails_closed(monkeypatch):
    users = FakeUsers(find_error=RuntimeError("mongo unavailable"))
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "reports_collection", FakeReports())

    assert database.can_submit_report(42) is False
    assert database.seconds_until_next_report(42) == database.REPORT_COOLDOWN_SECONDS


def test_malformed_report_cooldown_never_fails_open(monkeypatch):
    users = FakeUsers(entry={"_id": "42", "last_report_at": "broken"})
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "reports_collection", FakeReports())

    assert database.can_submit_report(42) is False
    assert database.seconds_until_next_report(42) == database.REPORT_COOLDOWN_SECONDS


def test_valid_report_cooldown_allows_only_after_window(monkeypatch):
    users = FakeUsers(entry={"_id": "42", "last_report_at": _now() - timedelta(seconds=30)})
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "reports_collection", FakeReports())
    monkeypatch.setattr(database, "_now_utc", _now)

    assert database.can_submit_report(42) is False
    assert database.seconds_until_next_report(42) == 30

    users.entry["last_report_at"] = _now() - timedelta(seconds=61)
    assert database.can_submit_report(42) is True
    assert database.seconds_until_next_report(42) == 0


def test_future_report_cooldown_timestamp_fails_closed(monkeypatch):
    users = FakeUsers(entry={"_id": "42", "last_report_at": _now() + timedelta(seconds=5)})
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "reports_collection", FakeReports())
    monkeypatch.setattr(database, "_now_utc", _now)

    assert database.can_submit_report(42) is False
    assert database.seconds_until_next_report(42) == database.REPORT_COOLDOWN_SECONDS


def test_failed_report_insert_returns_none_and_does_not_consume_cooldown(monkeypatch):
    users = FakeUsers(entry={"_id": "42"})
    reports = FakeReports(insert_error=RuntimeError("insert failed"))
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "reports_collection", reports)
    monkeypatch.setattr(database, "_now_utc", _now)

    report_id = database.insert_report(42, "u", "User", "bug", "text")

    assert report_id is None
    assert reports.inserted == []
    assert users.update_calls == []


def test_successful_report_insert_updates_cooldown_after_persistence(monkeypatch):
    users = FakeUsers(entry={"_id": "42"})
    reports = FakeReports()
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "reports_collection", reports)
    monkeypatch.setattr(database, "_now_utc", _now)

    report_id = database.insert_report(
        42,
        "u",
        "User",
        "bug",
        "text",
        context={"mode": "easy"},
    )

    assert isinstance(report_id, str) and report_id
    assert len(reports.inserted) == 1
    stored = reports.inserted[0]
    assert stored["_id"] == report_id
    assert stored["admin_delivered"] is False
    assert stored["context"] == {"mode": "easy"}
    assert users.update_calls == [
        (
            {"_id": "42"},
            {"$set": {"last_report_at": _now()}},
        )
    ]


def test_persisted_report_remains_success_when_secondary_cooldown_write_fails(monkeypatch):
    users = FakeUsers(entry={"_id": "42"}, update_error=RuntimeError("cooldown failed"))
    reports = FakeReports()
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "reports_collection", reports)
    monkeypatch.setattr(database, "_now_utc", _now)

    report_id = database.insert_report(42, "u", "User", "idea", "text")

    assert isinstance(report_id, str) and report_id
    assert len(reports.inserted) == 1
    assert reports.inserted[0]["_id"] == report_id
    assert len(users.update_calls) == 1


def test_zero_modified_cooldown_does_not_turn_durable_report_into_failure(monkeypatch):
    users = FakeUsers(entry={"_id": "42"}, modified_count=0)
    reports = FakeReports()
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "reports_collection", reports)
    monkeypatch.setattr(database, "_now_utc", _now)

    report_id = database.insert_report(42, "u", "User", "question", "text")

    assert isinstance(report_id, str) and report_id
    assert len(reports.inserted) == 1


def test_mark_report_delivered_is_idempotent(monkeypatch):
    reports = FakeReports(modified_count=1)
    monkeypatch.setattr(database, "reports_collection", reports)

    assert database.mark_report_delivered("r1") is True
    assert reports.update_calls[0][0] == {
        "_id": "r1",
        "admin_delivered": {"$ne": True},
    }

    reports.modified_count = 0
    reports.existing = {"_id": "r1", "admin_delivered": True}
    assert database.mark_report_delivered("r1") is True


def test_mark_report_delivered_failure_is_explicit(monkeypatch):
    reports = FakeReports(update_error=RuntimeError("write failed"))
    monkeypatch.setattr(database, "reports_collection", reports)

    assert database.mark_report_delivered("r1") is False
