from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

import database
import legacy_delivery_terminal as terminal
from battle_integrity import BATTLE_DELIVERY_PROTOCOL_OUTBOX


def _get(doc, path):
    current = doc
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current[part]
    return current, True


def _set(doc, path, value):
    current = doc
    parts = path.split(".")
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = deepcopy(value)


def _unset(doc, path):
    current = doc
    parts = path.split(".")
    for part in parts[:-1]:
        if not isinstance(current, dict) or part not in current:
            return
        current = current[part]
    if isinstance(current, dict):
        current.pop(parts[-1], None)


def _matches(doc, query):
    for path, expected in query.items():
        actual, exists = _get(doc, path)
        if isinstance(expected, dict) and "$ne" in expected:
            if exists and actual == expected["$ne"]:
                return False
            continue
        if not exists or actual != expected:
            return False
    return True


class Collection:
    def __init__(self, docs):
        self.docs = {doc["_id"]: deepcopy(doc) for doc in docs}

    def find_one(self, query, projection=None):
        del projection
        for doc in self.docs.values():
            if _matches(doc, query):
                return deepcopy(doc)
        return None

    def update_one(self, query, update):
        for doc in self.docs.values():
            if not _matches(doc, query):
                continue
            before = deepcopy(doc)
            for path, value in update.get("$set", {}).items():
                _set(doc, path, value)
            for path in update.get("$unset", {}):
                _unset(doc, path)
            return SimpleNamespace(modified_count=int(doc != before))
        return SimpleNamespace(modified_count=0)


def test_report_permanent_failure_is_terminal_and_retention_compatible(monkeypatch):
    now = datetime(2026, 8, 12, 13, 0, 0)
    reports = Collection(
        [
            {
                "_id": "r1",
                "admin_delivered": False,
                "delivery": {
                    "photo": {
                        "delivered": False,
                        "attempts": 1,
                        "claim_token": "photo-token",
                        "lease_until": now,
                    },
                    "text": {"delivered": True, "attempts": 1},
                },
            }
        ]
    )
    monkeypatch.setattr(database, "reports_collection", reports)
    monkeypatch.setattr(database, "_now_utc", lambda: now)

    assert terminal.settle_report_delivery_stage_failure(
        "r1",
        "photo",
        "photo-token",
        error="BadRequest: invalid file id",
    ) is True

    doc = reports.docs["r1"]
    photo = doc["delivery"]["photo"]
    assert photo["delivered"] is True
    assert photo["terminal_failed"] is True
    assert photo["terminal_error"] == "BadRequest: invalid file id"
    assert photo["terminal_at"] == now
    assert "claim_token" not in photo
    assert "lease_until" not in photo
    assert doc["admin_delivered"] is True
    assert doc["admin_delivery_failed"] is True


def test_battle_permanent_failure_settles_existing_delivered_guard(monkeypatch):
    now = datetime(2026, 8, 12, 13, 0, 0)
    battles = Collection(
        [
            {
                "_id": "b1",
                "creator_id": 10,
                "opponent_id": 20,
                "final_claimed": True,
                "result_delivery_protocol": BATTLE_DELIVERY_PROTOCOL_OUTBOX,
                "result_delivery": {
                    "creator": {
                        "delivered": False,
                        "attempts": 1,
                        "claim_token": "creator-token",
                        "lease_until": now,
                    },
                    "opponent": {"delivered": True, "attempts": 1},
                },
            }
        ]
    )
    monkeypatch.setattr(database, "battles_collection", battles)
    monkeypatch.setattr(database, "_now_utc", lambda: now)

    assert terminal.settle_battle_result_delivery_failure(
        "b1",
        10,
        "creator-token",
        error="Forbidden: blocked",
    ) is True

    creator = battles.docs["b1"]["result_delivery"]["creator"]
    assert creator["delivered"] is True
    assert creator["terminal_failed"] is True
    assert creator["terminal_error"] == "Forbidden: blocked"
    assert creator["terminal_at"] == now
    assert "claim_token" not in creator
    assert "lease_until" not in creator
