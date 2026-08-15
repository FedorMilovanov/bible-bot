import asyncio
from types import SimpleNamespace

import pytest
from telegram.error import NetworkError, RetryAfter

import telegram_battle_ready_delivery as ready


def run(coro):
    return asyncio.run(coro)


class Bot:
    def __init__(self):
        self.calls = []
        self.error = None

    async def send_message(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(message_id=len(self.calls))


def _claim():
    return {
        "battle": {
            "_id": "battle_0123456789abcdef",
            "creator_id": 42,
            "opponent_name": "Opponent",
        },
        "marker": {"delivered": False},
        "claim_token": "lease-token",
    }


def payload(battle_id, role):
    return f"start_battle_{battle_id}_{role}"


def test_successful_send_is_acknowledged(monkeypatch):
    bot = Bot()
    monkeypatch.setattr(ready, "claim_creator_ready_delivery", lambda *_args, **_kwargs: _claim())
    acknowledged = []
    monkeypatch.setattr(
        ready,
        "mark_creator_ready_delivered",
        lambda battle_id, token: acknowledged.append((battle_id, token)) or True,
    )

    assert run(
        ready.deliver_creator_ready_once(
            bot,
            "battle_0123456789abcdef",
            start_payload_builder=payload,
        )
    ) is True
    assert bot.calls[0]["chat_id"] == 42
    assert "Opponent" in bot.calls[0]["text"]
    assert bot.calls[0]["reply_markup"].inline_keyboard[0][0].callback_data == (
        "start_battle_battle_0123456789abcdef_creator"
    )
    assert acknowledged == [("battle_0123456789abcdef", "lease-token")]


def test_retry_after_becomes_durable_defer_without_sleep(monkeypatch):
    bot = Bot()
    bot.error = RetryAfter(23)
    monkeypatch.setattr(ready, "claim_creator_ready_delivery", lambda *_args, **_kwargs: _claim())
    deferred = []
    monkeypatch.setattr(
        ready,
        "defer_creator_ready_delivery",
        lambda battle_id, token, **kwargs: deferred.append((battle_id, token, kwargs)) or True,
    )

    assert run(
        ready.deliver_creator_ready_once(
            bot,
            "battle_0123456789abcdef",
            start_payload_builder=payload,
        )
    ) is False
    assert deferred[0][0:2] == ("battle_0123456789abcdef", "lease-token")
    assert deferred[0][2]["delay_seconds"] == 23.0
    assert "RetryAfter" in deferred[0][2]["error"]


def test_transient_network_error_releases_lease_for_recovery(monkeypatch):
    bot = Bot()
    bot.error = NetworkError("temporary")
    monkeypatch.setattr(ready, "claim_creator_ready_delivery", lambda *_args, **_kwargs: _claim())
    released = []
    monkeypatch.setattr(
        ready,
        "release_creator_ready_delivery",
        lambda battle_id, token, **kwargs: released.append((battle_id, token, kwargs)) or True,
    )

    with pytest.raises(NetworkError):
        run(
            ready.deliver_creator_ready_once(
                bot,
                "battle_0123456789abcdef",
                start_payload_builder=payload,
            )
        )
    assert released[0][0:2] == ("battle_0123456789abcdef", "lease-token")
    assert "NetworkError" in released[0][2]["error"]


def test_drain_recovers_pending_battle_after_restart(monkeypatch):
    bot = Bot()
    monkeypatch.setattr(
        ready,
        "get_pending_creator_ready_battles",
        lambda limit: [{"_id": "battle_0123456789abcdef"}],
    )
    delivered = []

    async def deliver(bot_arg, battle_id, *, start_payload_builder):
        assert bot_arg is bot
        assert start_payload_builder is payload
        delivered.append(battle_id)
        return True

    monkeypatch.setattr(ready, "deliver_creator_ready_once", deliver)

    summary = run(ready.drain_creator_ready_outbox(bot, start_payload_builder=payload, limit=10))
    assert summary.battles_seen == 1
    assert summary.delivered == 1
    assert summary.deferred == 0
    assert summary.errors == ()
    assert delivered == ["battle_0123456789abcdef"]
