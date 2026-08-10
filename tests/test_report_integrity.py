from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from pymongo.errors import DuplicateKeyError, PyMongoError

import database
from report_integrity import (
    ReportStoreUnavailable,
    accept_report_once,
    claim_report_delivery_stage,
    mark_report_delivery_stage_delivered,
    release_report_delivery_stage,
)


def _get_path(doc, path):
    current = doc
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current[part]
    return current, True


def _set_path(doc, path, value):
    current = doc
    parts = path.split(".")
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = deepcopy(value)


def _unset_path(doc, path):
    current = doc
    parts = path.split(".")
    for part in parts[:-1]:
        if not isinstance(current, dict) or part not in current:
            return
        current = current[part]
    if isinstance(current, dict):
        current.pop(parts[-1], None)


def _matches(doc, query):
    for key, expected in query.items():
        if key == "$or":
            if not any(_matches(doc, branch) for branch in expected):
                return False
            continue
        actual, exists = _get_path(doc, key)
        if isinstance(expected, dict):
            if "$exists" in expected and exists != expected["$exists"]:
                return False
            if "$ne" in expected and exists and actual == expected["$ne"]:
                return False
            if "$lte" in expected and (not exists or actual > expected["$lte"]):
                return False
            continue
        if not exists or actual != expected:
            return False
    return True


def _apply_update(doc, update):
    for path, amount in update.get("$inc", {}).items():
        current, exists = _get_path(doc, path)
        _set_path(doc, path, (current if exists else 0) + amount)
    for path, value in update.get("$set", {}).items():
        _set_path(doc, path, value)
    for path, value in update.get("$max", {}).items():
        current, exists = _get_path(doc, path)
        if not exists or current < value:
            _set_path(doc, path, value)
    for path in update.get("$unset", {}):
        _unset_path(doc, path)


class ReportCollection:
    def __init__(self):
        self.docs = {}

    def insert_one(self, doc):
        if doc["_id"] in self.docs:
            raise DuplicateKeyError("duplicate")
        self.docs[doc["_id"]] = deepcopy(doc)
        return SimpleNamespace(inserted_id=doc["_id"])

    def find_one(self, query, projection=None):
        doc = self.docs.get(query.get("_id"))
        if doc is None or not _matches(doc, query):
            return None
        return deepcopy(doc)

    def find_one_and_update(self, query, update, return_document=None):
        doc = self.docs.get(query.get("_id"))
        if doc is None or not _matches(doc, query):
            return None
        _apply_update(doc, update)
        return deepcopy(doc)

    def update_one(self, query, update):
        doc = self.docs.get(query.get("_id"))
        if doc is None or not _matches(doc, query):
            return SimpleNamespace(modified_count=0)
        before = deepcopy(doc)
        _apply_update(doc, update)
        return SimpleNamespace(modified_count=int(doc != before))


class UserCollection:
    def __init__(self, *, last_report_at=None):
        self.doc = {"_id": "42"}
        if last_report_at is not None:
            self.doc["last_report_at"] = last_report_at
        self.fail_update = False

    def update_one(self, query, update):
        if self.fail_update:
            raise PyMongoError("mongo user write failed")
        if query.get("_id") != self.doc["_id"]:
            return SimpleNamespace(modified_count=0)
        before = deepcopy(self.doc)
        _apply_update(self.doc, update)
        return SimpleNamespace(modified_count=int(self.doc != before))

    def find_one(self, query, projection=None):
        if query.get("_id") != self.doc["_id"]:
            return None
        return deepcopy(self.doc)


def _install(monkeypatch, *, now=None, last_report_at=None):
    now = now or datetime(2026, 8, 10, 12, 0, 0)
    reports = ReportCollection()
    users = UserCollection(last_report_at=last_report_at)
    monkeypatch.setattr(database, "reports_collection", reports)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: now)
    return reports, users, now


def _accept(**overrides):
    params = {
        "report_id": "r1",
        "user_id": 42,
        "username": "user",
        "first_name": "User",
        "report_type": "bug",
        "text": "Something broke",
        "photo_file_id": "telegram-photo-id",
        "context": {"mode": "hard"},
    }
    params.update(overrides)
    return accept_report_once(**params)


def test_acceptance_persists_attachment_delivery_state_and_cooldown(monkeypatch):
    reports, users, now = _install(monkeypatch)

    stored = _accept()

    assert stored["photo_file_id"] == "telegram-photo-id"
    assert stored["delivery"] == {
        "photo": {"delivered": False, "attempts": 0},
        "text": {"delivered": False, "attempts": 0},
    }
    assert reports.docs["r1"]["text"] == "Something broke"
    assert users.doc["last_report_at"] == now


def test_report_without_photo_marks_photo_stage_complete_at_acceptance(monkeypatch):
    _install(monkeypatch)

    stored = _accept(photo_file_id=None)

    assert stored["delivery"]["photo"]["delivered"] is True
    assert stored["delivery"]["text"]["delivered"] is False


def test_same_report_id_replay_is_idempotent_and_does_not_rewind_newer_cooldown(monkeypatch):
    newer = datetime(2026, 8, 11, 9, 0, 0)
    reports, users, _now = _install(monkeypatch, last_report_at=newer)

    first = _accept()
    replay = _accept()

    assert first["_id"] == replay["_id"] == "r1"
    assert len(reports.docs) == 1
    assert users.doc["last_report_at"] == newer


def test_same_report_id_with_different_content_fails_closed(monkeypatch):
    _install(monkeypatch)
    _accept()

    with pytest.raises(ReportStoreUnavailable, match="different immutable content"):
        _accept(text="different report")


def test_durable_report_survives_secondary_cooldown_failure_for_retry(monkeypatch):
    reports, users, _now = _install(monkeypatch)
    users.fail_update = True

    with pytest.raises(ReportStoreUnavailable):
        _accept()

    assert reports.docs["r1"]["text"] == "Something broke"
    users.fail_update = False
    replay = _accept()
    assert replay["_id"] == "r1"
    assert users.doc["last_report_at"] == reports.docs["r1"]["created_at_dt"]


def test_photo_and_text_are_independently_leased_and_acknowledged(monkeypatch):
    reports, _users, now = _install(monkeypatch)
    _accept()

    photo = claim_report_delivery_stage("r1", "photo", lease_seconds=60)
    text = claim_report_delivery_stage("r1", "text", lease_seconds=60)

    assert photo is not None and text is not None
    assert claim_report_delivery_stage("r1", "photo", lease_seconds=60) is None
    assert reports.docs["r1"]["delivery"]["photo"]["lease_until"] == now + timedelta(seconds=60)

    assert mark_report_delivery_stage_delivered("r1", "photo", photo["claim_token"]) is True
    assert reports.docs["r1"]["admin_delivered"] is False
    assert mark_report_delivery_stage_delivered("r1", "text", text["claim_token"]) is True
    assert reports.docs["r1"]["admin_delivered"] is True
    assert reports.docs["r1"]["admin_delivered_at"] == now


def test_wrong_delivery_token_cannot_ack_stage(monkeypatch):
    reports, _users, _now = _install(monkeypatch)
    _accept()
    claim = claim_report_delivery_stage("r1", "text")
    assert claim is not None

    assert mark_report_delivery_stage_delivered("r1", "text", "wrong") is False
    assert reports.docs["r1"]["delivery"]["text"]["delivered"] is False


def test_failed_stage_release_keeps_error_and_allows_retry(monkeypatch):
    reports, _users, _now = _install(monkeypatch)
    _accept()
    first = claim_report_delivery_stage("r1", "text")
    assert first is not None

    assert release_report_delivery_stage(
        "r1", "text", first["claim_token"], error="telegram timeout"
    ) is True
    state = reports.docs["r1"]["delivery"]["text"]
    assert state["last_error"] == "telegram timeout"
    assert "claim_token" not in state
    assert "lease_until" not in state

    second = claim_report_delivery_stage("r1", "text")
    assert second is not None
    assert second["claim_token"] != first["claim_token"]
    assert reports.docs["r1"]["delivery"]["text"]["attempts"] == 2
