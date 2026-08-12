from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

import database
import legacy_report_delivery_repair as repair


class Reports:
    def __init__(self, doc):
        self.doc = deepcopy(doc)
        self.updates = []

    def find_one(self, query, projection=None):
        del projection
        if query.get("_id") != self.doc.get("_id"):
            return None
        return deepcopy(self.doc)

    def update_one(self, query, update):
        self.updates.append((deepcopy(query), deepcopy(update)))
        if query.get("_id") != self.doc.get("_id"):
            return SimpleNamespace(modified_count=0)
        if query.get("admin_delivered", {}).get("$ne") is True and self.doc.get("admin_delivered") is True:
            return SimpleNamespace(modified_count=0)
        before = deepcopy(self.doc)
        for key, value in update.get("$set", {}).items():
            self.doc[key] = deepcopy(value)
        return SimpleNamespace(modified_count=int(self.doc != before))


def test_both_settled_stages_repair_missing_aggregate(monkeypatch):
    now = datetime(2026, 8, 12, 14, 0, 0)
    reports = Reports(
        {
            "_id": "r1",
            "admin_delivered": False,
            "delivery": {
                "photo": {"delivered": True, "terminal_failed": True},
                "text": {"delivered": True},
            },
        }
    )
    monkeypatch.setattr(database, "reports_collection", reports)
    monkeypatch.setattr(database, "_now_utc", lambda: now)

    assert repair.repair_report_delivery_aggregate("r1") is True
    assert reports.doc["admin_delivered"] is True
    assert reports.doc["admin_delivered_at"] == now
    assert reports.doc["admin_delivery_failed"] is True


def test_pending_stage_does_not_promote_aggregate(monkeypatch):
    reports = Reports(
        {
            "_id": "r1",
            "admin_delivered": False,
            "delivery": {
                "photo": {"delivered": True},
                "text": {"delivered": False},
            },
        }
    )
    monkeypatch.setattr(database, "reports_collection", reports)

    assert repair.repair_report_delivery_aggregate("r1") is False
    assert reports.updates == []
    assert reports.doc["admin_delivered"] is False


def test_existing_aggregate_is_idempotent_and_write_free(monkeypatch):
    reports = Reports(
        {
            "_id": "r1",
            "admin_delivered": True,
            "delivery": {
                "photo": {"delivered": True},
                "text": {"delivered": True},
            },
        }
    )
    monkeypatch.setattr(database, "reports_collection", reports)

    assert repair.repair_report_delivery_aggregate("r1") is True
    assert reports.updates == []
