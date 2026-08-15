import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest
from telegram.error import RetryAfter

import telegram_command_menu_retry as retry


def run(coro):
    return asyncio.run(coro)


class Bot:
    def __init__(self, error=None):
        self.error = error
        self.calls = []

    async def set_my_commands(self, commands):
        self.calls.append(commands)
        if self.error is not None:
            raise self.error
        return True


class JobQueue:
    def __init__(self):
        self.calls = []

    def run_once(self, callback, *, when):
        self.calls.append((callback, when))


async def retry_callback(context):
    del context


def test_success_does_not_schedule_retry():
    queue = JobQueue()
    context = SimpleNamespace(bot=Bot(), job_queue=queue)
    commands = ("menu", "test")

    assert run(retry.sync_public_commands_once(context, commands, retry_callback)) is True
    assert context.bot.calls == [commands]
    assert queue.calls == []


def test_retry_after_seconds_is_rescheduled_once():
    queue = JobQueue()
    context = SimpleNamespace(bot=Bot(RetryAfter(17)), job_queue=queue)

    assert run(retry.sync_public_commands_once(context, ("menu",), retry_callback)) is False
    assert queue.calls == [(retry_callback, 17.0)]


def test_retry_after_timedelta_uses_shared_normalizer():
    queue = JobQueue()
    context = SimpleNamespace(
        bot=Bot(RetryAfter(timedelta(seconds=23))),
        job_queue=queue,
    )

    assert run(retry.sync_public_commands_once(context, ("menu",), retry_callback)) is False
    assert queue.calls == [(retry_callback, 23.0)]


def test_retry_after_without_job_queue_fails_closed():
    context = SimpleNamespace(bot=Bot(RetryAfter(5)), job_queue=None)

    with pytest.raises(retry.CommandMenuRetryUnavailable, match="JobQueue"):
        run(retry.sync_public_commands_once(context, ("menu",), retry_callback))


def test_non_rate_limit_error_is_not_hidden_or_rescheduled():
    queue = JobQueue()
    context = SimpleNamespace(bot=Bot(RuntimeError("network")), job_queue=queue)

    with pytest.raises(RuntimeError, match="network"):
        run(retry.sync_public_commands_once(context, ("menu",), retry_callback))
    assert queue.calls == []
