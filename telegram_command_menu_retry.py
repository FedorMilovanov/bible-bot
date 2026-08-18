"""Retry policy for the idempotent Telegram public command/menu/profile sync."""
from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from telegram import MenuButtonWebApp, WebAppInfo
from telegram.error import RetryAfter

import telegram_main_menu as main_menu
import telegram_public_profile as public_profile
from telegram_delivery_retry import retry_after_seconds
from telegram_provider_status import main_mini_app_status

logger = logging.getLogger(__name__)


class CommandMenuRetryUnavailable(RuntimeError):
    """The public Telegram surface sync cannot reschedule after RetryAfter."""


def _desired_menu_button() -> MenuButtonWebApp | None:
    url = main_menu.current_miniapp_url()
    if not url:
        return None
    return MenuButtonWebApp(
        text="🚀 Открыть приложение",
        web_app=WebAppInfo(url=url),
    )


def _log_main_mini_app_provider(bot) -> None:
    """Log BotFather-owned Main Mini App state from PTB's cached getMe user."""
    try:
        status = main_mini_app_status(bot.bot)
    except RuntimeError:
        logger.warning(
            "Telegram Main Mini App provider status unavailable: bot is not initialized"
        )
        return

    username = status["username"] or "unknown"
    if status["has_main_web_app"]:
        logger.info("Telegram Main Mini App verified for @%s", username)
    else:
        logger.warning(
            "Telegram Main Mini App is not configured for @%s; configure it in BotFather",
            username,
        )


async def sync_public_commands_once(
    context,
    commands: Sequence[Any],
    retry_callback: Callable[[Any], Awaitable[Any]],
) -> bool:
    """Synchronize commands, public profile and default Mini App menu button once.

    Public profile and Mini App menu state are read/compare/write so already-correct
    Telegram provider state is left untouched. RetryAfter from any Bot API step is
    rescheduled instead of being lost until restart.

    Returns ``True`` when the public Telegram surface is synchronized and ``False``
    when a single future retry was scheduled. Non-rate-limit failures deliberately
    propagate so the production root keeps its existing logging policy.
    """
    try:
        await context.bot.set_my_commands(commands)
        await public_profile.sync_public_profile(context.bot)
        desired_button = _desired_menu_button()
        if desired_button is not None:
            current_button = await context.bot.get_chat_menu_button()
            if current_button != desired_button:
                await context.bot.set_chat_menu_button(menu_button=desired_button)
    except RetryAfter as exc:
        job_queue = getattr(context, "job_queue", None)
        if job_queue is None:
            raise CommandMenuRetryUnavailable(
                "public Telegram surface RetryAfter cannot be rescheduled without JobQueue"
            ) from exc
        delay = retry_after_seconds(exc)
        job_queue.run_once(retry_callback, when=delay)
        return False

    _log_main_mini_app_provider(context.bot)
    return True
