from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import legacy_battle_ready_delivery as delivery
from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE


def _battle():
    return {
        "_id": "battle_0123456789abcdef",
        "creator_id": 42,
        "opponent_id": 99,
        "opponent_name": "Opponent",
        "creator_finished": False,
        "status": "in_progress",
        "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
        "creator_ready_delivery": delivery.battle_ready_delivery_marker(),
    }


def test_future_opponent_claim_marker_is_minimal_and_pending():
    assert delivery.battle_ready_delivery_marker() == {
        "protocol": delivery.BATTLE_READY_DELIVERY_PROTOCOL,
        "delivered": False,
        "attempts": 0,
    }


def test_claim_is_bound_to_open_creator_and_protocol(monkeypatch):
    collection = Mock()
    collection.find_one_and_update.return_value = _battle()
    now = datetime(2026, 8, 15, 16, 0, 0)
    monkeypatch.setattr(delivery, "_collection", lambda: collection)
    monkeypatch.setattr(delivery, "_database", lambda: SimpleNamespace(_now_utc=lambda: now))
    monkeypatch.setattr(delivery.uuid, "uuid4", lambda: SimpleNamespace(hex="lease-token"))

    claim = delivery.claim_creator_ready_delivery("battle_0123456789abcdef")

    assert claim["claim_token"] == "lease-token"
    query = collection.find_one_and_update.call_args.args[0]
    assert query["status"] == "in_progress"
    assert query["creator_finished"] == {"$ne": True}
    assert query["question_progress_protocol"] == BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE
    assert query["creator_ready_delivery.protocol"] == delivery.BATTLE_READY_DELIVERY_PROTOCOL
    update = collection.find_one_and_update.call_args.args[1]
    assert update["$inc"] == {"creator_ready_delivery.attempts": 1}
    assert update["$set"]["creator_ready_delivery.claim_token"] == "lease-token"


def test_retry_after_is_persisted_as_due_time(monkeypatch):
    collection = Mock()
    collection.update_one.return_value = SimpleNamespace(modified_count=1)
    now = datetime(2026, 8, 15, 16, 0, 0)
    monkeypatch.setattr(delivery, "_collection", lambda: collection)
    monkeypatch.setattr(delivery, "_database", lambda: SimpleNamespace(_now_utc=lambda: now))

    assert delivery.defer_creator_ready_delivery(
        "battle_0123456789abcdef",
        "lease-token",
        delay_seconds=17.5,
        error="RetryAfter",
    ) is True

    update = collection.update_one.call_args.args[1]
    assert update["$set"]["creator_ready_delivery.retry_after"].timestamp() == pytest.approx(
        now.timestamp() + 17.5
    )
    assert update["$set"]["creator_ready_delivery.last_error"] == "RetryAfter"
    assert "creator_ready_delivery.claim_token" in update["$unset"]
    assert "creator_ready_delivery.lease_until" in update["$unset"]


def test_ack_is_idempotent_after_lost_response(monkeypatch):
    collection = Mock()
    collection.update_one.return_value = SimpleNamespace(modified_count=0)
    collection.find_one.return_value = {
        "creator_ready_delivery": {
            "protocol": delivery.BATTLE_READY_DELIVERY_PROTOCOL,
            "delivered": True,
        }
    }
    monkeypatch.setattr(delivery, "_collection", lambda: collection)
    monkeypatch.setattr(
        delivery,
        "_database",
        lambda: SimpleNamespace(_now_utc=lambda: datetime(2026, 8, 15, 16, 0, 0)),
    )

    assert delivery.mark_creator_ready_delivered(
        "battle_0123456789abcdef",
        "lease-token",
    ) is True


def test_pending_lookup_requires_marker_and_in_progress_status(monkeypatch):
    cursor = Mock()
    cursor.sort.return_value = cursor
    cursor.limit.return_value = []
    collection = Mock()
    collection.find.return_value = cursor
    now = datetime(2026, 8, 15, 16, 0, 0)
    monkeypatch.setattr(delivery, "_collection", lambda: collection)
    monkeypatch.setattr(delivery, "_database", lambda: SimpleNamespace(_now_utc=lambda: now))

    assert delivery.get_pending_creator_ready_battles(25) == []
    query = collection.find.call_args.args[0]
    assert query["status"] == "in_progress"
    assert query["creator_finished"] == {"$ne": True}
    assert query["creator_ready_delivery.protocol"] == delivery.BATTLE_READY_DELIVERY_PROTOCOL
    assert query["creator_ready_delivery.delivered"] == {"$ne": True}
