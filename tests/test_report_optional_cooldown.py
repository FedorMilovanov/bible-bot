from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

import pytest
from pymongo.errors import PyMongoError

import database
import legacy_inaccuracy_report as inaccuracy
import report_integrity as reports


class ReportCollection:
    def __init__(self):
        self.docs = {}

    def insert_one(self, doc):
        self.docs[doc["_id"]] = deepcopy(doc)
        return SimpleNamespace(inserted_id=doc["_id"])

    def find_one(self, query, projection=None):
        doc = self.docs.get(query.get("_id"))
        return deepcopy(doc) if doc is not None else None


class FailingUsers:
    def __init__(self):
        self.update_calls = 0

    def update_one(self, query, update):
        self.update_calls += 1
        raise PyMongoError("cooldown writer unavailable")

    def find_one(self, query, projection=None):
        return None


def _install(monkeypatch):
    report_store = ReportCollection()
    users = FailingUsers()
    monkeypatch.setattr(database, "reports_collection", report_store)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 11, 1, 0, 0))
    return report_store, users


def _accept(*, update_cooldown=True):
    return reports.accept_report_once(
        report_id="r1",
        user_id=42,
        username="user",
        first_name="User",
        report_type="bug",
        text="Broken",
        context={"kind": "test"},
        update_cooldown=update_cooldown,
    )


def test_opt_out_report_is_durable_without_touching_user_cooldown(monkeypatch):
    report_store, users = _install(monkeypatch)

    stored = _accept(update_cooldown=False)

    assert stored["_id"] == "r1"
    assert report_store.docs["r1"]["text"] == "Broken"
    assert users.update_calls == 0


def test_normal_report_still_requires_durable_cooldown_followup(monkeypatch):
    report_store, users = _install(monkeypatch)

    with pytest.raises(reports.ReportStoreUnavailable, match="report acceptance failed"):
        _accept(update_cooldown=True)

    assert report_store.docs["r1"]["text"] == "Broken"
    assert users.update_calls == 1


def test_inaccuracy_report_explicitly_preserves_no_cooldown_policy(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        inaccuracy,
        "accept_report_once",
        lambda **kwargs: captured.update(kwargs) or {"_id": kwargs["report_id"]},
    )

    inaccuracy.accept_inaccuracy_report_once(
        user_id=42,
        username="user",
        first_name="User",
        attempt_id="attempt-1",
        question_index=0,
        question={
            "question": "Who?",
            "options": ["Peter", "Paul"],
            "correct": 1,
        },
        level_name="Easy",
    )

    assert captured["update_cooldown"] is False
