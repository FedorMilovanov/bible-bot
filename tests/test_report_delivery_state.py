from types import SimpleNamespace

import pytest
from pymongo.errors import AutoReconnect

import database
from report_integrity import ReportStoreUnavailable, get_report_delivery_stage_state


class Reports:
    def __init__(self, doc=None, error=None):
        self.doc = doc
        self.error = error
        self.query = None
        self.projection = None

    def find_one(self, query, projection=None):
        self.query = query
        self.projection = projection
        if self.error is not None:
            raise self.error
        return self.doc


def install(monkeypatch, reports):
    monkeypatch.setattr(database, "reports_collection", reports)
    monkeypatch.setattr(database, "collection", SimpleNamespace())


def test_stage_lookup_distinguishes_delivered_from_active_lease(monkeypatch):
    reports = Reports({
        "_id": "r1",
        "photo_file_id": "photo",
        "delivery": {
            "photo": {
                "delivered": False,
                "attempts": 2,
                "claim_token": "other",
                "lease_until": "later",
            }
        },
    })
    install(monkeypatch, reports)

    state = get_report_delivery_stage_state("r1", "photo")

    assert state == {
        "report_id": "r1",
        "stage": "photo",
        "delivered": False,
        "claim_token": "other",
        "lease_until": "later",
        "attempts": 2,
        "photo_file_id": "photo",
    }
    assert reports.query == {"_id": "r1"}


def test_stage_lookup_returns_none_for_missing_report(monkeypatch):
    reports = Reports(None)
    install(monkeypatch, reports)
    assert get_report_delivery_stage_state("missing", "text") is None


def test_missing_stage_state_fails_closed(monkeypatch):
    reports = Reports({"_id": "r1", "delivery": {}})
    install(monkeypatch, reports)

    with pytest.raises(ReportStoreUnavailable, match="stage state is missing"):
        get_report_delivery_stage_state("r1", "text")


def test_stage_lookup_mongo_outage_is_explicit(monkeypatch):
    reports = Reports(error=AutoReconnect("mongo unavailable"))
    install(monkeypatch, reports)

    with pytest.raises(ReportStoreUnavailable, match="state lookup failed"):
        get_report_delivery_stage_state("r1", "text")
