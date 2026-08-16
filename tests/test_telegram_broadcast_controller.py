import asyncio
import os
from types import SimpleNamespace

import pytest
from pymongo.errors import AutoReconnect
from telegram.error import Forbidden, NetworkError, RetryAfter

os.environ.setdefault("ADMIN_USER_ID", "1")

import database
import telegram_broadcast_controller as broadcasts
from broadcast_integrity import BroadcastStoreUnavailable


class Message:
    def __init__(self, text="/broadcast hello", chat_id=1):
        self.text = text
        self.chat_id = chat_id
        self.replies = []

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class Update:
    def __init__(self, *, user_id=1, update_id=42, text="/broadcast hello"):
        self.update_id = update_id
        self.effective_user = SimpleNamespace(id=user_id)
        self.message = Message(text=text, chat_id=user_id)


class Bot:
    def __init__(self, error=None):
        self.error = error
        self.sent = []

    async def send_message(self, *, chat_id, text):
        self.sent.append((chat_id, text))
        if self.error is not None:
            raise self.error
        return SimpleNamespace(message_id=len(self.sent))


def run(coro):
    return asyncio.run(coro)


def test_recipient_snapshot_distinguishes_empty_collection_from_outage(monkeypatch):
    class Users:
        def __init__(self, docs=None, error=None):
            self.docs = docs or []
            self.error = error

        def find(self, _query, _projection):
            if self.error is not None:
                raise self.error
            return list(self.docs)

    monkeypatch.setattr(database, "collection", Users([]))
    assert broadcasts._recipient_ids_strict() == []

    monkeypatch.setattr(database, "collection", None)
    with pytest.raises(BroadcastStoreUnavailable, match="recipient storage"):
        broadcasts._recipient_ids_strict()

    monkeypatch.setattr(database, "collection", Users(error=AutoReconnect("down")))
    with pytest.raises(BroadcastStoreUnavailable, match="snapshot failed"):
        broadcasts._recipient_ids_strict()


def test_non_admin_cannot_accept_broadcast(monkeypatch):
    monkeypatch.setattr(broadcasts, "_admin_user_id", lambda: 1)
    monkeypatch.setattr(
        broadcasts,
        "accept_broadcast_once",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not accept")),
    )
    update = Update(user_id=999)

    run(broadcasts.broadcast_command(update, object()))

    assert update.message.replies == [("❌ Нет доступа.", {})]


def test_admin_command_uses_update_id_and_durable_recipient_snapshot(monkeypatch):
    monkeypatch.setattr(broadcasts, "_admin_user_id", lambda: 1)
    monkeypatch.setattr(broadcasts, "get_broadcast", lambda _broadcast_id: None)
    monkeypatch.setattr(broadcasts, "_recipient_ids_strict", lambda: [10, 20])
    monkeypatch.setattr(
        broadcasts,
        "ensure_broadcast_fanout",
        lambda _stored: (_ for _ in ()).throw(
            AssertionError("command acceptance must not materialize fanout")
        ),
    )
    captured = []

    def accept(**kwargs):
        captured.append(kwargs)
        return ({"recipient_count": 2, "fanout_ready": False}, True)

    monkeypatch.setattr(broadcasts, "accept_broadcast_once", accept)
    update = Update(update_id=77, text="/broadcast Important news")

    run(broadcasts.broadcast_command(update, object()))

    assert captured == [
        {
            "broadcast_id": "telegram_update_77",
            "admin_id": 1,
            "admin_chat_id": 1,
            "text": "Important news",
            "recipient_ids": [10, 20],
        }
    ]
    assert "durable-очередь: 2" in update.message.replies[0][0]


def test_same_update_replay_uses_stored_snapshot_without_resnapshotting_users(monkeypatch):
    monkeypatch.setattr(broadcasts, "_admin_user_id", lambda: 1)
    existing = {
        "_id": "telegram_update_77",
        "admin_id": "1",
        "admin_chat_id": "1",
        "text": "Important news",
        "recipient_ids": ["10", "20"],
        "recipient_count": 2,
        "fanout_ready": False,
    }
    monkeypatch.setattr(broadcasts, "get_broadcast", lambda _broadcast_id: existing)
    monkeypatch.setattr(
        broadcasts,
        "_recipient_ids_strict",
        lambda: (_ for _ in ()).throw(AssertionError("replay must not resnapshot users")),
    )
    monkeypatch.setattr(
        broadcasts,
        "accept_broadcast_once",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("replay must not reaccept")),
    )
    monkeypatch.setattr(
        broadcasts,
        "ensure_broadcast_fanout",
        lambda _stored: (_ for _ in ()).throw(
            AssertionError("replay acknowledgement must not depend on fanout")
        ),
    )
    update = Update(update_id=77, text="/broadcast Important news")

    run(broadcasts.broadcast_command(update, object()))

    assert "уже принята: 2" in update.message.replies[0][0]


def test_same_update_replay_rejects_different_immutable_content(monkeypatch):
    monkeypatch.setattr(broadcasts, "_admin_user_id", lambda: 1)
    existing = {
        "_id": "telegram_update_77",
        "admin_id": "1",
        "admin_chat_id": "1",
        "text": "Original",
        "recipient_ids": ["10"],
        "recipient_count": 1,
        "fanout_ready": False,
    }
    monkeypatch.setattr(broadcasts, "get_broadcast", lambda _broadcast_id: existing)
    update = Update(update_id=77, text="/broadcast Changed")

    run(broadcasts.broadcast_command(update, object()))

    assert "Broadcast status is unknown" in update.message.replies[0][0]


def test_admin_command_fails_closed_before_delivery_when_snapshot_unavailable(monkeypatch):
    monkeypatch.setattr(broadcasts, "_admin_user_id", lambda: 1)
    monkeypatch.setattr(broadcasts, "get_broadcast", lambda _broadcast_id: None)
    monkeypatch.setattr(
        broadcasts,
        "_recipient_ids_strict",
        lambda: (_ for _ in ()).throw(BroadcastStoreUnavailable("mongo down")),
    )
    monkeypatch.setattr(
        broadcasts,
        "accept_broadcast_once",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not accept")),
    )
    update = Update()

    run(broadcasts.broadcast_command(update, object()))

    assert "Broadcast status is unknown" in update.message.replies[0][0]


def _install_one_delivery(monkeypatch, *, send_error=None):
    delivery = {
        "_id": "telegram_update_1:10",
        "broadcast_id": "telegram_update_1",
        "claim_token": "claim",
        "user_id": "10",
    }
    claims = iter([delivery, None])
    monkeypatch.setattr(
        broadcasts,
        "get_pending_broadcasts",
        lambda limit=20: [
            {"_id": "telegram_update_1", "fanout_ready": True, "text": "hello"}
        ],
    )
    monkeypatch.setattr(
        broadcasts,
        "claim_next_broadcast_delivery",
        lambda **_kwargs: next(claims),
    )
    monkeypatch.setattr(
        broadcasts,
        "get_broadcast",
        lambda _broadcast_id: {"_id": "telegram_update_1", "text": "hello"},
    )
    monkeypatch.setattr(broadcasts, "BROADCAST_SLEEP", 0)
    syncs = []
    monkeypatch.setattr(
        broadcasts,
        "sync_broadcast_completion",
        lambda broadcast_id: syncs.append(broadcast_id) or {"completed": False},
    )
    return Bot(error=send_error), delivery, syncs


def test_delivery_worker_materializes_pending_parent_before_claim(monkeypatch):
    parent = {
        "_id": "telegram_update_1",
        "fanout_ready": False,
        "text": "hello",
        "recipient_ids": ["10"],
    }
    monkeypatch.setattr(broadcasts, "get_pending_broadcasts", lambda limit=20: [parent])
    prepared = []
    monkeypatch.setattr(
        broadcasts,
        "ensure_broadcast_fanout",
        lambda stored: prepared.append(stored["_id"]) or {**stored, "fanout_ready": True},
    )
    monkeypatch.setattr(broadcasts, "claim_next_broadcast_delivery", lambda **_kwargs: None)
    monkeypatch.setattr(
        broadcasts,
        "sync_broadcast_completion",
        lambda _broadcast_id: {"completed": False},
    )

    summary = run(broadcasts.drain_broadcast_outbox(Bot(), limit=1))

    assert prepared == ["telegram_update_1"]
    assert summary.claimed == 0


def test_delivery_worker_acks_successful_recipient(monkeypatch):
    bot, delivery, syncs = _install_one_delivery(monkeypatch)
    acks = []
    monkeypatch.setattr(
        broadcasts,
        "mark_broadcast_delivery_delivered",
        lambda delivery_id, token: acks.append((delivery_id, token)) or True,
    )

    summary = run(broadcasts.drain_broadcast_outbox(bot, limit=5))

    assert summary.claimed == 1
    assert summary.delivered == 1
    assert summary.terminal_failed == 0
    assert acks == [(delivery["_id"], "claim")]
    assert bot.sent == [(10, "📢 Сообщение от автора бота:\n\nhello")]
    assert syncs == ["telegram_update_1"]


def test_permanent_telegram_failure_is_terminal_not_retried_forever(monkeypatch):
    bot, delivery, _syncs = _install_one_delivery(
        monkeypatch,
        send_error=Forbidden("blocked"),
    )
    terminal = []
    releases = []
    monkeypatch.setattr(
        broadcasts,
        "mark_broadcast_delivery_terminal_failure",
        lambda delivery_id, token, *, error: terminal.append((delivery_id, token, error)) or True,
    )
    monkeypatch.setattr(
        broadcasts,
        "release_broadcast_delivery",
        lambda *args, **kwargs: releases.append((args, kwargs)) or True,
    )

    summary = run(broadcasts.drain_broadcast_outbox(bot, limit=5))

    assert summary.terminal_failed == 1
    assert summary.deferred == 0
    assert terminal[0][0:2] == (delivery["_id"], "claim")
    assert releases == []


def test_retry_after_is_durably_deferred_without_second_send_or_release(monkeypatch):
    bot, delivery, _syncs = _install_one_delivery(
        monkeypatch,
        send_error=RetryAfter(300),
    )
    deferrals = []
    releases = []
    monkeypatch.setattr(
        broadcasts,
        "defer_broadcast_delivery",
        lambda delivery_id, token, *, delay_seconds, error: deferrals.append(
            (delivery_id, token, delay_seconds, error)
        )
        or True,
    )
    monkeypatch.setattr(
        broadcasts,
        "release_broadcast_delivery",
        lambda *args, **kwargs: releases.append((args, kwargs)) or True,
    )

    summary = run(broadcasts.drain_broadcast_outbox(bot, limit=5))

    assert summary.deferred == 1
    assert summary.delivered == 0
    assert len(bot.sent) == 1
    assert deferrals[0][0:3] == (delivery["_id"], "claim", 300.0)
    assert releases == []


def test_transient_network_failure_releases_lease_and_defers(monkeypatch):
    bot, delivery, _syncs = _install_one_delivery(
        monkeypatch,
        send_error=NetworkError("network down"),
    )
    releases = []
    monkeypatch.setattr(
        broadcasts,
        "release_broadcast_delivery",
        lambda delivery_id, token, *, error: releases.append((delivery_id, token, error)) or True,
    )

    summary = run(broadcasts.drain_broadcast_outbox(bot, limit=5))

    assert summary.deferred == 1
    assert summary.delivered == 0
    assert releases[0][0:2] == (delivery["_id"], "claim")
