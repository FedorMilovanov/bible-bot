from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

import pytest
from pymongo.errors import DuplicateKeyError, PyMongoError

import database
import report_integrity as reports


class ReportCollection:
    def __init__(self):
        self.doc = None

    def insert_one(self, doc):
        if self.doc is not None:
            raise DuplicateKeyError("duplicate")
        self.doc = deepcopy(doc)
        return SimpleNamespace(inserted_id=doc["_id"])

    def find_one(self, query, projection=None):
        if self.doc is None or query.get("_id") != self.doc.get("_id"):
            return None
        return deepcopy(self.doc)


class UserCollection:
    def __init__(self):
        self.doc = {"_id": "42"}
        self.fail_update = True

    def update_one(self, query, update):
        if self.fail_update:
            raise PyMongoError("cooldown write failed")
        if query.get("_id") != self.doc["_id"]:
            return SimpleNamespace(modified_count=0)
        value = update["$max"]["last_report_at"]
        previous = self.doc.get("last_report_at")
        if previous is None or previous < value:
            self.doc["last_report_at"] = value
            return SimpleNamespace(modified_count=1)
        return SimpleNamespace(modified_count=0)

    def find_one(self, query, projection=None):
        if query.get("_id") != self.doc["_id"]:
            return None
        return deepcopy(self.doc)


def _accept(*, context, text="Something broke"):
    return reports.accept_report_once(
        report_id="report-1",
        user_id=42,
        username="user",
        first_name="User",
        report_type="bug",
        text=text,
        photo_file_id="photo-id",
        context=context,
    )


def test_changed_retry_context_keeps_original_durable_snapshot(monkeypatch):
    now = datetime(2026, 8, 10, 12, 0, 0)
    report_store = ReportCollection()
    users = UserCollection()
    monkeypatch.setattr(database, "reports_collection", report_store)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: now)

    with pytest.raises(reports.ReportStoreUnavailable):
        _accept(context={"screen": "question", "question_index": 3})

    assert report_store.doc["context"] == {"screen": "question", "question_index": 3}

    users.fail_update = False
    replay = _accept(context={"screen": "menu"})

    assert replay["_id"] == "report-1"
    assert replay["context"] == {"screen": "question", "question_index": 3}
    assert users.doc["last_report_at"] == now

    with pytest.raises(reports.ReportStoreUnavailable, match="different immutable content"):
        _accept(context={"screen": "menu"}, text="different report")
