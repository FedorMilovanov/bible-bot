from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import legacy_result_card_delivery as delivery


def _session(**overrides):
    value = {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "chat_id": 777,
        "correct_count": 2,
        "question_ids": ["q1", "q2", "q3"],
        "level_name": "1 Петра 1",
        "mode": "level",
        "is_retry": False,
    }
    value.update(overrides)
    return value


def test_marker_contains_only_truthful_terminal_fallback_evidence():
    marker = delivery.build_result_card_delivery_marker(_session())

    assert marker == {
        "protocol": delivery.RESULT_CARD_DELIVERY_PROTOCOL,
        "delivered": False,
        "attempts": 0,
        "chat_id": 777,
        "score": 2,
        "total": 3,
        "level_name": "1 Петра 1",
        "mode": "level",
        "is_retry": False,
    }
    assert "points" not in marker
    assert "bonus" not in marker
    assert "position" not in marker


def test_missing_chat_destination_never_blocks_terminal_scoring():
    assert delivery.build_result_card_delivery_marker(_session(chat_id=None)) is None


def test_malformed_chat_destination_is_not_silently_persisted():
    with pytest.raises(delivery.ResultCardDeliveryConflict, match="chat_id"):
        delivery.build_result_card_delivery_marker(_session(chat_id=False))


def test_rich_text_write_is_idempotent_after_lost_response(monkeypatch):
    collection = Mock()
    collection.find_one_and_update.return_value = None
    collection.find_one.return_value = {
        "status": "finished",
        "result_card_delivery": {
            "protocol": delivery.RESULT_CARD_DELIVERY_PROTOCOL,
            "delivered": False,
            "text": "rich result",
        },
    }
    monkeypatch.setattr(delivery, "_collection", lambda: collection)
    monkeypatch.setattr(delivery, "_owner_id", lambda user_id: str(user_id))

    assert delivery.set_result_card_delivery_text("s1", 42, "rich result") is True


def test_conflicting_rich_text_replay_fails_closed(monkeypatch):
    collection = Mock()
    collection.find_one_and_update.return_value = None
    collection.find_one.return_value = {
        "status": "finished",
        "result_card_delivery": {
            "protocol": delivery.RESULT_CARD_DELIVERY_PROTOCOL,
            "delivered": False,
            "text": "first result",
        },
    }
    monkeypatch.setattr(delivery, "_collection", lambda: collection)
    monkeypatch.setattr(delivery, "_owner_id", lambda user_id: str(user_id))

    with pytest.raises(delivery.ResultCardDeliveryConflict, match="conflicts"):
        delivery.set_result_card_delivery_text("s1", 42, "different result")


def test_retry_after_is_persisted_as_utc_due_time(monkeypatch):
    collection = Mock()
    collection.update_one.return_value = SimpleNamespace(modified_count=1)
    now = datetime(2026, 8, 15, 12, 0, 0)
    monkeypatch.setattr(delivery, "_collection", lambda: collection)
    monkeypatch.setattr(delivery, "_owner_id", lambda user_id: str(user_id))
    monkeypatch.setattr(
        delivery,
        "_database",
        lambda: SimpleNamespace(_now_utc=lambda: now),
    )

    assert delivery.defer_result_card_delivery(
        "s1",
        42,
        "lease-token",
        delay_seconds=17.5,
        error="RetryAfter",
    ) is True

    update = collection.update_one.call_args.args[1]
    assert update["$set"]["result_card_delivery.retry_after"] == now + timedelta(
        seconds=17.5
    )
    assert update["$set"]["result_card_delivery.last_error"] == "RetryAfter"
    assert "result_card_delivery.claim_token" in update["$unset"]
    assert "result_card_delivery.lease_until" in update["$unset"]
