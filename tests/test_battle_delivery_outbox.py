from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace

import database
from battle_integrity import (
    claim_battle_result_delivery,
    mark_battle_result_delivered,
    release_battle_result_delivery,
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
    for key, value in update.get("$inc", {}).items():
        current, exists = _get_path(doc, key)
        _set_path(doc, key, (current if exists else 0) + value)
    for key, value in update.get("$set", {}).items():
        _set_path(doc, key, value)
    for key in update.get("$unset", {}):
        _unset_path(doc, key)


class DeliveryCollection:
    def __init__(self, doc):
        self.doc = deepcopy(doc)

    def find_one(self, query, projection=None):
        if not _matches(self.doc, query):
            return None
        return deepcopy(self.doc)

    def find_one_and_update(self, query, update, return_document=None):
        if not _matches(self.doc, query):
            return None
        _apply_update(self.doc, update)
        return deepcopy(self.doc)

    def update_one(self, query, update):
        if not _matches(self.doc, query):
            return SimpleNamespace(modified_count=0)
        _apply_update(self.doc, update)
        return SimpleNamespace(modified_count=1)


def _finalized():
    return {
        "_id": "b1",
        "creator_id": 10,
        "creator_name": "Creator",
        "opponent_id": 20,
        "opponent_name": "Opponent",
        "creator_finished": True,
        "opponent_finished": True,
        "final_claimed": True,
        "status": "finalized",
        "result_delivery": {
            "creator": {"delivered": False, "attempts": 0},
            "opponent": {"delivered": False, "attempts": 0},
        },
    }


def test_delivery_lease_blocks_concurrent_duplicate_and_counts_attempt(monkeypatch):
    now = datetime(2026, 8, 10, 12, 0, 0)
    collection = DeliveryCollection(_finalized())
    monkeypatch.setattr(database, "battles_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: now)

    first = claim_battle_result_delivery("b1", 10, lease_seconds=120)
    second = claim_battle_result_delivery("b1", 10, lease_seconds=120)

    assert first is not None
    assert first["role"] == "creator"
    assert isinstance(first["claim_token"], str) and first["claim_token"]
    assert second is None
    state = collection.doc["result_delivery"]["creator"]
    assert state["attempts"] == 1
    assert state["lease_until"] == now + timedelta(seconds=120)
    assert state["claim_token"] == first["claim_token"]


def test_delivery_ack_requires_matching_token_and_is_idempotent(monkeypatch):
    now = datetime(2026, 8, 10, 12, 0, 0)
    collection = DeliveryCollection(_finalized())
    monkeypatch.setattr(database, "battles_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: now)

    claim = claim_battle_result_delivery("b1", 20)
    assert claim is not None

    assert mark_battle_result_delivered("b1", 20, "wrong-token") is False
    assert collection.doc["result_delivery"]["opponent"]["delivered"] is False

    assert mark_battle_result_delivered("b1", 20, claim["claim_token"]) is True
    state = collection.doc["result_delivery"]["opponent"]
    assert state["delivered"] is True
    assert state["delivered_at"] == now
    assert "claim_token" not in state
    assert "lease_until" not in state

    assert mark_battle_result_delivered("b1", 20, claim["claim_token"]) is True


def test_failed_delivery_release_allows_immediate_retry(monkeypatch):
    now = datetime(2026, 8, 10, 12, 0, 0)
    collection = DeliveryCollection(_finalized())
    monkeypatch.setattr(database, "battles_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: now)

    first = claim_battle_result_delivery("b1", 10)
    assert first is not None
    assert release_battle_result_delivery(
        "b1", 10, first["claim_token"], error="telegram unavailable"
    ) is True

    state = collection.doc["result_delivery"]["creator"]
    assert "claim_token" not in state
    assert "lease_until" not in state
    assert state["last_error"] == "telegram unavailable"

    second = claim_battle_result_delivery("b1", 10)
    assert second is not None
    assert second["claim_token"] != first["claim_token"]
    assert collection.doc["result_delivery"]["creator"]["attempts"] == 2


def test_expired_lease_can_be_reclaimed(monkeypatch):
    now = datetime(2026, 8, 10, 12, 0, 0)
    doc = _finalized()
    doc["result_delivery"]["creator"].update(
        {
            "attempts": 1,
            "claim_token": "old",
            "lease_until": now - timedelta(seconds=1),
        }
    )
    collection = DeliveryCollection(doc)
    monkeypatch.setattr(database, "battles_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: now)

    claim = claim_battle_result_delivery("b1", 10)

    assert claim is not None
    assert claim["claim_token"] != "old"
    assert collection.doc["result_delivery"]["creator"]["attempts"] == 2


def test_nonparticipant_cannot_claim_or_ack_delivery(monkeypatch):
    collection = DeliveryCollection(_finalized())
    monkeypatch.setattr(database, "battles_collection", collection)

    assert claim_battle_result_delivery("b1", 99) is None
    assert mark_battle_result_delivered("b1", 99, "token") is False
