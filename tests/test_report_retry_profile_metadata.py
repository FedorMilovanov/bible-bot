from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

import pytest
from pymongo.errors import DuplicateKeyError, PyMongoError

import report_integrity as reports_api


class Reports:
    def __init__(self):
        self.docs = {}

    def insert_one(self, doc):
        if doc["_id"] in self.docs:
            raise DuplicateKeyError("duplicate")
        self.docs[doc["_id"]] = deepcopy(doc)
        return SimpleNamespace(inserted_id=doc["_id"])

    def find_one(self, query, projection=None):
        doc = self.docs.get(query.get("_id"))
        return deepcopy(doc) if doc is not None else None


class Users:
    def __init__(self):
        self.doc = {"_id": "42"}
        self.fail = True

    def update_one(self, query, update):
        if self.fail:
            raise PyMongoError("cooldown write failed")
        value = update["$max"]["last_report_at"]
        before = self.doc.get("last_report_at")
        if before is None or before < value:
            self.doc["last_report_at"] = value
            return SimpleNamespace(modified_count=1)
        return SimpleNamespace(modified_count=0)

    def find_one(self, query, projection=None):
        if query.get("_id") != "42":
            return None
        return deepcopy(self.doc)


def _accept(**overrides):
    kwargs = {
        "report_id": "r1",
        "user_id": 42,
        "username": "old_user",
        "first_name": "Old Name",
        "report_type": "bug",
        "text": "Something broke",
        "photo_file_id": "photo-1",
        "context": {"mode": "hard"},
    }
    kwargs.update(overrides)
    return reports_api.accept_report_once(**kwargs)


def test_retry_repairs_cooldown_even_if_profile_metadata_changed(monkeypatch):
    now = datetime(2026, 8, 10, 12, 0, 0)
    reports = Reports()
    users = Users()
    database = SimpleNamespace(_now_utc=lambda: now, _uid=lambda value: str(value))
    monkeypatch.setattr(reports_api, "_collections", lambda: (database, reports, users))

    with pytest.raises(reports_api.ReportStoreUnavailable, match="acceptance failed"):
        _accept()

    assert reports.docs["r1"]["username"] == "old_user"
    users.fail = False

    replay = _accept(username="new_user", first_name="New Name")

    assert replay["_id"] == "r1"
    assert replay["username"] == "old_user"
    assert replay["first_name"] == "Old Name"
    assert users.doc["last_report_at"] == now


def test_retry_still_rejects_changed_report_content(monkeypatch):
    now = datetime(2026, 8, 10, 12, 0, 0)
    reports = Reports()
    users = Users()
    database = SimpleNamespace(_now_utc=lambda: now, _uid=lambda value: str(value))
    monkeypatch.setattr(reports_api, "_collections", lambda: (database, reports, users))

    with pytest.raises(reports_api.ReportStoreUnavailable):
        _accept()
    users.fail = False

    with pytest.raises(reports_api.ReportStoreUnavailable, match="different immutable content"):
        _accept(username="new_user", text="Different content")
