import os

import pytest

os.environ.setdefault("ADMIN_USER_ID", "1")

import telegram_broadcast_controller as broadcasts
from broadcast_integrity import BroadcastStoreUnavailable


def _stored_parent():
    return {
        "_id": "telegram_update_77",
        "admin_id": "1",
        "admin_chat_id": "1",
        "text": "Important news",
        "recipient_ids": ["10", "20"],
        "recipient_count": 2,
        "fanout_ready": False,
    }


def test_ambiguous_acceptance_error_recovers_exact_parent_by_deterministic_id(monkeypatch):
    monkeypatch.setattr(broadcasts, "_recipient_ids_strict", lambda: [10, 20])
    monkeypatch.setattr(
        broadcasts,
        "accept_broadcast_once",
        lambda **_kwargs: (_ for _ in ()).throw(
            BroadcastStoreUnavailable("write acknowledgement lost")
        ),
    )
    reads = []

    def read_back(broadcast_id):
        reads.append(broadcast_id)
        return _stored_parent()

    monkeypatch.setattr(broadcasts, "get_broadcast", read_back)

    stored, created, recipients = broadcasts._accept_or_recover_new_broadcast(
        broadcast_id="telegram_update_77",
        admin_id=1,
        admin_chat_id=1,
        text="Important news",
    )

    assert reads == ["telegram_update_77"]
    assert stored == _stored_parent()
    assert created is False
    assert recipients == ["10", "20"]


def test_ambiguous_acceptance_without_exact_parent_stays_fail_closed(monkeypatch):
    monkeypatch.setattr(broadcasts, "_recipient_ids_strict", lambda: [10])
    monkeypatch.setattr(
        broadcasts,
        "accept_broadcast_once",
        lambda **_kwargs: (_ for _ in ()).throw(
            BroadcastStoreUnavailable("write acknowledgement lost")
        ),
    )
    monkeypatch.setattr(broadcasts, "get_broadcast", lambda _broadcast_id: None)

    with pytest.raises(BroadcastStoreUnavailable, match="acknowledgement lost"):
        broadcasts._accept_or_recover_new_broadcast(
            broadcast_id="telegram_update_77",
            admin_id=1,
            admin_chat_id=1,
            text="Important news",
        )


def test_ambiguous_acceptance_readback_rejects_different_immutable_parent(monkeypatch):
    monkeypatch.setattr(broadcasts, "_recipient_ids_strict", lambda: [10])
    monkeypatch.setattr(
        broadcasts,
        "accept_broadcast_once",
        lambda **_kwargs: (_ for _ in ()).throw(
            BroadcastStoreUnavailable("write acknowledgement lost")
        ),
    )
    wrong = _stored_parent()
    wrong["text"] = "Different"
    monkeypatch.setattr(broadcasts, "get_broadcast", lambda _broadcast_id: wrong)

    with pytest.raises(BroadcastStoreUnavailable, match="different immutable content"):
        broadcasts._accept_or_recover_new_broadcast(
            broadcast_id="telegram_update_77",
            admin_id=1,
            admin_chat_id=1,
            text="Important news",
        )
