import asyncio

import pytest

import telegram_result_delivery_controller as controller


def run(coro):
    return asyncio.run(coro)


class Bot:
    def __init__(self):
        self.calls = []

    async def send_message(self, *args, **kwargs):
        self.calls.append((args, kwargs))


def test_pre_outbox_finished_session_keeps_legacy_direct_delivery(monkeypatch):
    bot = Bot()

    async def missing_marker(*_args, **_kwargs):
        raise controller.ResultCardDeliveryConflict("result-card outbox marker is missing")

    monkeypatch.setattr(controller, "deliver_live_result_card", missing_marker)
    proxy = controller._ResultCardBotProxy(bot, session_id="old-session", user_id=42)

    run(proxy.send_message(chat_id=777, text="legacy result", parse_mode="Markdown"))

    assert bot.calls == [
        ((), {"chat_id": 777, "text": "legacy result", "parse_mode": "Markdown"})
    ]


def test_contradictory_outbox_state_never_falls_back_to_duplicate_direct_send(monkeypatch):
    bot = Bot()

    async def conflicting_marker(*_args, **_kwargs):
        raise controller.ResultCardDeliveryConflict(
            "result-card text conflicts with durable evidence"
        )

    monkeypatch.setattr(controller, "deliver_live_result_card", conflicting_marker)
    proxy = controller._ResultCardBotProxy(bot, session_id="s1", user_id=42)

    with pytest.raises(controller.ResultCardDeliveryConflict, match="conflicts"):
        run(proxy.send_message(chat_id=777, text="different result"))

    assert bot.calls == []
