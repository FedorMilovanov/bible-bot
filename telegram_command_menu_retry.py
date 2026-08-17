"""Retry policy for the idempotent Telegram public command/menu sync."""
from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from telegram import MenuButtonWebApp, WebAppInfo
from telegram.error import RetryAfter

import telegram_main_menu as main_menu
from telegram_delivery_retry import retry_after_seconds


class CommandMenuRetryUnavailable(RuntimeError):
    """The public Telegram menu sync cannot reschedule after RetryAfter."""


def _desired_menu_button() -> MenuButtonWebApp | None:
    url = main_menu.current_miniapp_url()
    if not url:
        return None
    return MenuButtonWebApp(
        text="🚀 Открыть приложение",
        web_app=WebAppInfo(url=url),
    )


async def sync_public_commands_once(
    context,
    commands: Sequence[Any],
    retry_callback: Callable[[Any], Awaitable[Any]],
) -> bool:
    """Synchronize commands and the default Mini App menu button once.

    The Mini App button is read/compare/write so an already-correct Telegram
    provider state is left untouched. RetryAfter from any Bot API step is
    rescheduled instead of being lost until restart.

    Returns ``True`` when the public command/menu surface is synchronized and
    ``False`` when a single future retry was scheduled. Non-rate-limit failures
    deliberately propagate so the production root keeps its existing logging
    policy.
    """
    try:
        await context.bot.set_my_commands(commands)
        desired_button = _desired_menu_button()
        if desired_button is not None:
            current_button = await context.bot.get_chat_menu_button()
            if current_button != desired_button:
                await context.bot.set_chat_menu_button(menu_button=desired_button)
    except RetryAfter as exc:
        job_queue = getattr(context, "job_queue", None)
        if job_queue is None:
            raise CommandMenuRetryUnavailable(
                "public command/menu RetryAfter cannot be rescheduled without JobQueue"
            ) from exc
        delay = retry_after_seconds(exc)
        job_queue.run_once(retry_callback, when=delay)
        return False
    return True
