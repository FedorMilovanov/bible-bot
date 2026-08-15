import asyncio
from datetime import timedelta

from telegram.error import RetryAfter

import utils


def _run(coro):
    return asyncio.run(coro)


def test_safe_send_retries_timedelta_retry_after_without_type_error(monkeypatch):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    class Target:
        def __init__(self):
            self.calls = 0

        async def reply_text(self, text, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise RetryAfter(timedelta(seconds=2))
            return "sent"

    target = Target()
    monkeypatch.setattr(utils.asyncio, "sleep", fake_sleep)

    assert _run(utils.safe_send(target, "hello")) == "sent"
    assert target.calls == 2
    assert sleeps == [2.5]


def test_safe_edit_retries_timedelta_retry_after_without_type_error(monkeypatch):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    class Query:
        def __init__(self):
            self.calls = 0

        async def edit_message_text(self, text, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise RetryAfter(timedelta(seconds=3))
            return "edited"

    query = Query()
    monkeypatch.setattr(utils.asyncio, "sleep", fake_sleep)

    assert _run(utils.safe_edit(query, "hello")) == "edited"
    assert query.calls == 2
    assert sleeps == [3.5]
