"""Translate Telegram RetryAfter into the generic durable-delivery defer signal."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from telegram.error import RetryAfter

from legacy_delivery_worker import LegacyDeliveryDeferred


def retry_after_seconds(exc: RetryAfter) -> float:
    value = exc.retry_after
    if isinstance(value, timedelta):
        return max(1.0, value.total_seconds())
    try:
        return max(1.0, float(value))
    except (TypeError, ValueError):
        return 1.0


async def send_with_durable_retry_after(
    sender: Callable[..., Awaitable[Any]],
    *args,
) -> Any:
    try:
        return await sender(*args)
    except RetryAfter as exc:
        delay = retry_after_seconds(exc)
        raise LegacyDeliveryDeferred(
            delay,
            detail=f"RetryAfter: {exc}",
        ) from exc
