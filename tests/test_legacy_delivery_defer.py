from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace

import database
import legacy_delivery_defer as defer
from battle_integrity import BATTLE_DELIVERY_PROTOCOL_OUTBOX


class Collection:
    def __init__(self, *, find_result=None):
        self.find_result = deepcopy(find_result)
        self.updates = []

    def find_one(self, query):
        self.find_query = deepcopy(query)
        return deepcopy(self.find_result)

    def update_one(self, query, update):
        self.updates.append((deepcopy(query), deepcopy(update)))
        return SimpleNamespace(modified_count=1)


def test_report_deferral_keeps_future_lease_and_removes_claim(monkeypatch):
    now = datetime(2026, 8, 12, 12, 0, 0)
    reports = Collection()
    monkeypatch.setattr(database, "reports_collection", reports)
    monkeypatch.setattr(database, "_now_utc", lambda: now)

    assert defer.defer_report_delivery_stage(
        "r1",
        "photo",
        "tok",
        delay_seconds=300,
        error="RetryAfter",
    ) is True

    query, update = reports.updates[0]
    assert query == {
        "_id": "r1",
        "delivery.photo.delivered": {"$ne": True},
        "delivery.photo.claim_token": "tok",
    }
    assert update["$set"] == {
        "delivery.photo.lease_until": now + timedelta(seconds=300),
        "delivery.photo.last_error": "RetryAfter",
    }
    assert update["$unset"] == {"delivery.photo.claim_token": ""}
    assert "delivery.photo.lease_until" not in update["$unset"]


def test_battle_deferral_keeps_future_lease_and_removes_claim(monkeypatch):
    now = datetime(2026, 8, 12, 12, 0, 0)
    battle = {
        "_id": "b1",
        "creator_id": 10,
        "opponent_id": 20,
        "final_claimed": True,
        "result_delivery_protocol": BATTLE_DELIVERY_PROTOCOL_OUTBOX,
    }
    battles = Collection(find_result=battle)
    monkeypatch.setattr(database, "battles_collection", battles)
    monkeypatch.setattr(database, "_now_utc", lambda: now)

    assert defer.defer_battle_result_delivery(
        "b1",
        20,
        "tok",
        delay_seconds=180,
        error="RetryAfter",
    ) is True

    assert battles.find_query == {
        "_id": "b1",
        "final_claimed": True,
        "result_delivery_protocol": BATTLE_DELIVERY_PROTOCOL_OUTBOX,
    }
    query, update = battles.updates[0]
    assert query == {
        "_id": "b1",
        "result_delivery_protocol": BATTLE_DELIVERY_PROTOCOL_OUTBOX,
        "result_delivery.opponent.delivered": {"$ne": True},
        "result_delivery.opponent.claim_token": "tok",
    }
    assert update["$set"] == {
        "result_delivery.opponent.lease_until": now + timedelta(seconds=180),
        "result_delivery.opponent.last_error": "RetryAfter",
    }
    assert update["$unset"] == {"result_delivery.opponent.claim_token": ""}
    assert "result_delivery.opponent.lease_until" not in update["$unset"]


def test_deferral_rejects_nonfinite_or_nonpositive_delay():
    for value in (0, -1, float("inf"), float("nan"), True):
        try:
            defer._delay_seconds(value)
        except ValueError:
            pass
        else:
            raise AssertionError(f"invalid delay accepted: {value!r}")
