# ruff: noqa: RUF001
"""Production-wide Telegram error policy detached from the legacy bot module."""
from __future__ import annotations

import logging
import traceback
from collections.abc import Awaitable, Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, ChatMigrated, NetworkError, RetryAfter, TimedOut

from utils import safe_truncate

logger = logging.getLogger(__name__)


async def on_error(update: object, context, *, admin_user_id: int) -> None:
    """Apply the historical global error policy without legacy presentation authority."""
    err = context.error

    if isinstance(err, (NetworkError, TimedOut)):
        logger.debug("Network noise ignored: %s", err)
        return

    if isinstance(err, RetryAfter):
        logger.warning("RetryAfter: retry in %ss", err.retry_after)
        return

    if isinstance(err, BadRequest) and "not modified" in str(err).lower():
        return

    if isinstance(err, ChatMigrated):
        logger.info("ChatMigrated: new_chat_id=%s", err.new_chat_id)
        return

    tb = "".join(traceback.format_exception(type(err), err, err.__traceback__))
    logger.error("Unhandled exception:\n%s", tb)

    if not (isinstance(update, Update) and update.effective_user):
        return

    try:
        msg_target = update.message or (
            update.callback_query.message if update.callback_query else None
        )
        if msg_target:
            await msg_target.reply_text(
                "⚠️ Произошла ошибка. Нажми /reset или сообщи автору.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🆘 Сброс",
                                callback_data="reset_session",
                            ),
                            InlineKeyboardButton(
                                "🐞 Сообщить",
                                callback_data="report_start_bug_direct",
                            ),
                        ]
                    ]
                ),
            )
    except Exception:
        pass

    try:
        await context.bot.send_message(
            chat_id=admin_user_id,
            text=safe_truncate(f"🚨 ОШИБКА\n\n{tb[:1500]}"),
        )
    except Exception:
        pass


def build_error_handler(
    admin_user_id: int,
) -> Callable[[object, object], Awaitable[None]]:
    """Bind the configured administrator id once at composition time."""
    if isinstance(admin_user_id, bool) or not isinstance(admin_user_id, int):
        raise TypeError("admin_user_id must be an integer")

    async def handler(update: object, context) -> None:
        await on_error(update, context, admin_user_id=admin_user_id)

    return handler
