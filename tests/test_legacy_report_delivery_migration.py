from copy import deepcopy
from types import SimpleNamespace

import pytest
from pymongo.errors import AutoReconnect

import legacy_report_delivery_migration as migration


def _get_path(doc, path):
    current = doc
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current[part]
    return current, True


def _matches(doc, query):
    for key, expected in query.items():
        actual, exists = _get_path(doc, key)
        if isinstance(expected, dict):
            if "$exists" in expected and exists != expected["$exists"]:
                return False
            if "$ne" in expected and exists and actual == expected["$ne"]:
                return False
            continue
        if not exists or actual != expected:
            return False
    return True


class FakeReports:
    def __init__(self, doc):
        self.doc = deepcopy(doc)
        self.updates = []
        self.fail_read = False
        self.concurrent_delivery = None

    def find_one(self, query):
        if self.fail_read:
            raise AutoReconnect("mongo unavailable")
        if self.doc is None or not _matches(self.doc, query):
            return None
        return deepcopy(self.doc)

    def update_one(self, query, update):
        self.updates.append((deepcopy(query), deepcopy(update)))
        if self.concurrent_delivery is not None:
            self.doc["delivery"] = deepcopy(self.concurrent_delivery)
            self.concurrent_delivery = None
            return SimpleNamespace(modified_count=0)
        if self.doc is None or not _matches(self.doc, query):
            return SimpleNamespace(modified_count=0)
        self.doc["delivery"] = deepcopy(update["$set"]["delivery"])
        return SimpleNamespace(modified_count=1)


def _legacy(**overrides):
    doc = {
        "_id": "r1",
        "report_id": "r1",
        "admin_delivered": False,
        "text": "legacy report",
    }
    doc.update(overrides)
    return doc


def test_legacy_report_without_durable_photo_backfills_text_only_outbox(monkeypatch):
    reports = FakeReports(_legacy())
    monkeypatch.setattr(migration, "_collection", lambda: reports)

    doc = migration.ensure_report_delivery_state("r1")

    assert doc["delivery"] == {
        "photo": {"delivered": True, "attempts": 0},
        "text": {"delivered": False, "attempts": 0},
    }
    assert reports.doc["delivery"] == doc["delivery"]
    assert reports.updates[0][0] == {
        "_id": "r1",
        "admin_delivered": {"$ne": True},
        "delivery": {"$exists": False},
    }


def test_transitional_legacy_report_with_durable_photo_keeps_photo_obligation(monkeypatch):
    reports = FakeReports(_legacy(photo_file_id="telegram-photo"))
    monkeypatch.setattr(migration, "_collection", lambda: reports)

    doc = migration.ensure_report_delivery_state("r1")

    assert doc["delivery"]["photo"] == {"delivered": False, "attempts": 0}
    assert doc["delivery"]["text"] == {"delivered": False, "attempts": 0}


def test_blank_legacy_photo_id_is_rejected_before_backfill(monkeypatch):
    reports = FakeReports(_legacy(photo_file_id="   "))
    monkeypatch.setattr(migration, "_collection", lambda: reports)

    with pytest.raises(migration.LegacyReportDeliveryStateInvalid, match="photo_file_id"):
        migration.ensure_report_delivery_state("r1")

    assert reports.updates == []


def test_existing_valid_delivery_state_is_never_rewritten(monkeypatch):
    state = {
        "photo": {"delivered": True, "attempts": 2},
        "text": {"delivered": False, "attempts": 1},
    }
    reports = FakeReports(_legacy(delivery=state))
    monkeypatch.setattr(migration, "_collection", lambda: reports)

    doc = migration.ensure_report_delivery_state("r1")

    assert doc["delivery"] == state
    assert reports.updates == []


def test_partial_or_malformed_delivery_state_fails_closed_without_overwrite(monkeypatch):
    reports = FakeReports(
        _legacy(delivery={"photo": {"delivered": True, "attempts": 0}})
    )
    monkeypatch.setattr(migration, "_collection", lambda: reports)

    with pytest.raises(migration.LegacyReportDeliveryStateInvalid, match="text"):
        migration.ensure_report_delivery_state("r1")

    assert reports.updates == []


def test_pending_photo_without_durable_file_id_is_invalid(monkeypatch):
    reports = FakeReports(
        _legacy(
            delivery={
                "photo": {"delivered": False, "attempts": 0},
                "text": {"delivered": False, "attempts": 0},
            }
        )
    )
    monkeypatch.setattr(migration, "_collection", lambda: reports)

    with pytest.raises(migration.LegacyReportDeliveryStateInvalid, match="photo_file_id"):
        migration.ensure_report_delivery_state("r1")


def test_concurrent_backfill_winner_is_reread_and_accepted(monkeypatch):
    reports = FakeReports(_legacy())
    reports.concurrent_delivery = {
        "photo": {"delivered": True, "attempts": 0},
        "text": {"delivered": False, "attempts": 1},
    }
    monkeypatch.setattr(migration, "_collection", lambda: reports)

    doc = migration.ensure_report_delivery_state("r1")

    assert doc["delivery"] == reports.doc["delivery"]
    assert doc["delivery"]["text"]["attempts"] == 1


def test_already_admin_delivered_legacy_report_is_not_backfilled(monkeypatch):
    reports = FakeReports(_legacy(admin_delivered=True))
    monkeypatch.setattr(migration, "_collection", lambda: reports)

    doc = migration.ensure_report_delivery_state("r1")

    assert doc["admin_delivered"] is True
    assert "delivery" not in doc
    assert reports.updates == []


def test_compatible_claim_migrates_then_uses_normal_claim_store(monkeypatch):
    reports = FakeReports(_legacy())
    monkeypatch.setattr(migration, "_collection", lambda: reports)
    seen = {}

    def claim(report_id, stage, *, lease_seconds):
        seen.update(report_id=report_id, stage=stage, lease_seconds=lease_seconds)
        return {"claim_token": "token"}

    monkeypatch.setattr(migration, "_claim_stage", claim)

    result = migration.claim_report_delivery_stage_compatible(
        "r1", "text", lease_seconds=77
    )

    assert result == {"claim_token": "token"}
    assert seen == {"report_id": "r1", "stage": "text", "lease_seconds": 77}
    assert reports.doc["delivery"]["photo"]["delivered"] is True


def test_mongo_outage_is_explicit(monkeypatch):
    reports = FakeReports(_legacy())
    reports.fail_read = True
    monkeypatch.setattr(migration, "_collection", lambda: reports)

    with pytest.raises(
        migration.LegacyReportDeliveryMigrationUnavailable,
        match="migration failed",
    ):
        migration.ensure_report_delivery_state("r1")
