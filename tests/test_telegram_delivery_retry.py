import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest
from telegram.error import RetryAfter

import telegram_delivery_retry as retry
from legacy_delivery_worker import LegacyDeliveryDeferred


def run(coro):
    return asyncio.run(coro)


def test_retry_after_seconds_supports_numeric_and_timedelta_values():
    assert retry.retry_after_seconds(SimpleNamespace(retry_after=5)) == 5.0
    assert retry.retry_after_seconds(SimpleNamespace(retry_after=timedelta(seconds=7))) == 7.0
    assert retry.retry_after_seconds(SimpleNamespace(retry_after=0)) == 1.0


def test_sender_retry_after_becomes_generic_durable_defer_signal():
    calls = []

    async def sender(value):
        calls.append(value)
        raise RetryAfter(300)

    with pytest.raises(LegacyDeliveryDeferred) as caught:
        run(retry.send_with_durable_retry_after(sender, "payload"))

    assert calls == ["payload"]
    assert caught.value.delay_seconds == 300.0
    assert "RetryAfter" in caught.value.detail


def test_non_rate_limit_sender_error_passes_through_unchanged():
    error = RuntimeError("network down")

    async def sender():
        raise error

    with pytest.raises(RuntimeError) as caught:
        run(retry.send_with_durable_retry_after(sender))
    assert caught.value is error
