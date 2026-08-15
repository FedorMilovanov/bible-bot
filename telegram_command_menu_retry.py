"""Retry policy for the idempotent Telegram public command-menu sync."""
from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from telegram.error import RetryAfter

from telegram_delivery_retry import retry_after_seconds


class CommandMenuRetryUnavailable(RuntimeError):
    """The command menu received RetryAfter but no JobQueue can reschedule it."""


async def sync_public_commands_once(
    context,
    commands: Sequence[Any],
    retry_callback: Callable[[Any], Awaitable[Any]],
) -> bool:
    """Set commands once; RetryAfter is rescheduled instead of lost until restart.

    Returns ``True`` when Telegram accepted the command menu and ``False`` when
    a single future retry was scheduled. Non-rate-limit failures deliberately
    propagate so the production root keeps its existing logging policy.
    """
    try:
        await context.bot.set_my_commands(commands)
    except RetryAfter as exc:
        job_queue = getattr(context, "job_queue", None)
        if job_queue is None:
            raise CommandMenuRetryUnavailable(
                "command-menu RetryAfter cannot be rescheduled without JobQueue"
            ) from exc
        delay = retry_after_seconds(exc)
        job_queue.run_once(retry_callback, when=delay)
        return False
    return True
