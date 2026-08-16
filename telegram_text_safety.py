"""Focused Telegram text/edit safety primitives for production controllers."""
from __future__ import annotations

import asyncio
import logging

from telegram.error import BadRequest, RetryAfter, TimedOut

from telegram_delivery_retry import retry_after_seconds

logger = logging.getLogger(__name__)
MAX_MSG_LEN = 3900


def _close_open_tags(text: str) -> str:
    """Close simple Telegram Markdown tags after truncation."""
    stack: list[str] = []
    index = 0
    while index < len(text):
        if text[index : index + 3] == "```":
            if stack and stack[-1] == "```":
                stack.pop()
            else:
                stack.append("```")
            index += 3
            continue
        char = text[index]
        if char in ("*", "_", "`"):
            if stack and stack[-1] == char:
                stack.pop()
            else:
                stack.append(char)
        index += 1
    for tag in reversed(stack):
        text += tag
    return text


def safe_truncate(text: str, limit: int = MAX_MSG_LEN) -> str:
    """Truncate Telegram Markdown while preserving the historical behavior."""
    if not text:
        return ""
    if len(text) <= limit:
        return text

    cut_pos = limit - 3
    for separator in ("\n", ". ", " "):
        position = text.rfind(separator, 0, cut_pos)
        if position > cut_pos - 200:
            cut_pos = position
            break
    return _close_open_tags(text[:cut_pos] + "…")


async def safe_edit(query, text: str, **kwargs):
    """Edit a callback message with the existing retry/plain-text fallbacks."""
    text = safe_truncate(text)
    for attempt in range(3):
        try:
            return await query.edit_message_text(
                text,
                parse_mode="Markdown",
                **kwargs,
            )
        except RetryAfter as exc:
            delay = retry_after_seconds(exc)
            logger.warning("RetryAfter in safe_edit: %ss", delay)
            await asyncio.sleep(delay + 0.5)
        except BadRequest as exc:
            error = str(exc).lower()
            if "not modified" in error:
                return None
            if "can't parse" in error:
                kwargs.pop("parse_mode", None)
                try:
                    return await query.edit_message_text(text, **kwargs)
                except Exception as fallback_exc:
                    logger.error("safe_edit plain fallback failed: %s", fallback_exc)
                    return None
            logger.error("safe_edit BadRequest: %s", exc)
            return None
        except TimedOut:
            if attempt < 2:
                await asyncio.sleep(1)
            else:
                logger.error("safe_edit timed out after 3 attempts")
                return None
        except Exception as exc:
            logger.error("safe_edit failed: %s", exc)
            return None
    return None


__all__ = ["MAX_MSG_LEN", "safe_edit", "safe_truncate"]
