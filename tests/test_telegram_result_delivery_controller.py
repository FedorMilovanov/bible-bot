import asyncio
from types import SimpleNamespace

from telegram.error import RetryAfter

import telegram_result_delivery_controller as controller


def run(coro):
    return asyncio.run(coro)


class Bot:
    def __init__(self):
        self.calls = []
        self.error = None

    async def send_message(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.error is not None:
            raise self.error
        return SimpleNamespace(message_id=len(self.calls))


def _claim(*, text="*rich* result"):
    return {
        "session": {"_id": "s1", "user_id": "42"},
        "marker": {
            "chat_id": 777,
            "score": 2,
            "total": 3,
            "level_name": "1 Петра 1",
            "is_retry": False,
            **({"text": text} if text is not None else {}),
        },
        "claim_token": "token-1",
    }


def test_successful_remote_send_is_acknowledged(monkeypatch):
    bot = Bot()
    monkeypatch.setattr(controller, "claim_result_card_delivery", lambda *_args, **_kwargs: _claim())
    acknowledged = []
    monkeypatch.setattr(
        controller,
        "mark_result_card_delivered",
        lambda session_id, user_id, token: acknowledged.append((session_id, user_id, token)) or True,
    )

    assert run(controller.deliver_result_card_once(bot, "s1", 42)) is True
    assert bot.calls[0][1] == {
        "chat_id": 777,
        "text": "*rich* result",
        "parse_mode": "Markdown",
    }
    assert acknowledged == [("s1", 42, "token-1")]


def test_retry_after_becomes_durable_defer_without_sleep(monkeypatch):
    bot = Bot()
    bot.error = RetryAfter(23)
    monkeypatch.setattr(controller, "claim_result_card_delivery", lambda *_args, **_kwargs: _claim())
    deferred = []
    monkeypatch.setattr(
        controller,
        "defer_result_card_delivery",
        lambda session_id, user_id, token, **kwargs: deferred.append(
            (session_id, user_id, token, kwargs)
        ) or True,
    )

    assert run(controller.deliver_result_card_once(bot, "s1", 42)) is False
    assert deferred[0][:3] == ("s1", 42, "token-1")
    assert deferred[0][3]["delay_seconds"] == 23.0
    assert "RetryAfter" in deferred[0][3]["error"]


def test_restart_fallback_uses_only_durable_core_evidence(monkeypatch):
    bot = Bot()
    monkeypatch.setattr(
        controller,
        "claim_result_card_delivery",
        lambda *_args, **_kwargs: _claim(text=None),
    )
    monkeypatch.setattr(controller, "mark_result_card_delivered", lambda *_args: True)

    assert run(controller.deliver_result_card_once(bot, "s1", 42)) is True
    sent = bot.calls[0][1]
    assert sent["chat_id"] == 777
    assert "Правильно: 2/3" in sent["text"]
    assert "Категория: 1 Петра 1" in sent["text"]
    assert "parse_mode" not in sent
    assert "балл" not in sent["text"].lower()
    assert "бонус" not in sent["text"].lower()


def test_renderer_wrapper_intercepts_only_first_send(monkeypatch):
    bot = Bot()
    routed = []

    async def durable(bot_arg, *, session_id, user_id, text):
        assert bot_arg is bot
        routed.append((session_id, user_id, text))
        return True

    monkeypatch.setattr(controller, "deliver_live_result_card", durable)

    async def renderer(bot_arg, user_id, outcome, *, retry_drill=False):
        assert user_id == 42
        assert retry_drill is False
        await bot_arg.send_message(chat_id=777, text="result", parse_mode="Markdown")
        await bot_arg.send_message(chat_id=777, text="achievement")

    quiz = SimpleNamespace(_render_result=renderer)
    assert controller.install_result_card_renderer(quiz) is True
    assert controller.install_result_card_renderer(quiz) is False

    outcome = SimpleNamespace(session_id="s1")
    run(quiz._render_result(bot, 42, outcome))

    assert routed == [("s1", 42, "result")]
    assert bot.calls == [((), {"chat_id": 777, "text": "achievement"})]


def test_memory_only_renderer_remains_direct(monkeypatch):
    bot = Bot()

    async def renderer(bot_arg, user_id, outcome, *, retry_drill=False):
        await bot_arg.send_message(chat_id=777, text="memory result")

    quiz = SimpleNamespace(_render_result=renderer)
    controller.install_result_card_renderer(quiz)
    run(quiz._render_result(bot, 42, SimpleNamespace()))

    assert bot.calls == [((), {"chat_id": 777, "text": "memory result"})]
