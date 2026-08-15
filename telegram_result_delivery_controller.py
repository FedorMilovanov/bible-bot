"""Production adapter for crash-safe quiz result-card delivery.

The mature quiz controller still owns result wording, achievements and menus.
This adapter intercepts only the first ``send_message`` performed by its result
renderer for a persisted outcome, stores that exact rich card in the terminal
session outbox, and sends it under a durable lease. Later messages pass through
unchanged. Memory-only retry review remains completely untouched.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from legacy_delivery_worker import LegacyDeliveryDeferred, LegacyDeliveryPermanentFailure
from legacy_result_card_delivery import (
    ResultCardDeliveryConflict,
    ResultCardDeliveryUnavailable,
    claim_result_card_delivery,
    defer_result_card_delivery,
    get_pending_result_card_sessions,
    mark_result_card_delivered,
    release_result_card_delivery,
    set_result_card_delivery_text,
    settle_result_card_delivery_failure,
)
from telegram_delivery_retry import send_with_durable_retry_after

logger = logging.getLogger(__name__)


class ResultCardDeliveryAcknowledgementPending(RuntimeError):
    """Remote delivery may have happened but durable acknowledgement is pending."""


@dataclass(frozen=True)
class ResultCardDrainSummary:
    sessions_seen: int = 0
    delivered: int = 0
    deferred: int = 0
    errors: tuple[str, ...] = ()


def _fallback_text(marker: dict) -> str:
    score = marker.get("score")
    total = marker.get("total")
    level_name = marker.get("level_name")
    if (
        isinstance(score, bool)
        or not isinstance(score, int)
        or score < 0
        or isinstance(total, bool)
        or not isinstance(total, int)
        or total <= 0
        or score > total
    ):
        raise ResultCardDeliveryConflict("result-card fallback score is invalid")
    if not isinstance(level_name, str) or not level_name.strip():
        level_name = "Тест"
    prefix = "Повторение ошибок" if marker.get("is_retry") is True else "Результат сохранён"
    return (
        f"🏆 {prefix}\n\n"
        f"Категория: {level_name.strip()[:200]}\n"
        f"Правильно: {score}/{total}\n\n"
        "Статистика уже надёжно записана."
    )


def _claimed_payload(claim: dict) -> tuple[dict, str, bool, str]:
    if not isinstance(claim, dict):
        raise ResultCardDeliveryConflict("result-card claim is invalid")
    marker = claim.get("marker")
    token = claim.get("claim_token")
    if not isinstance(marker, dict):
        raise ResultCardDeliveryConflict("result-card claim marker is invalid")
    if not isinstance(token, str) or not token:
        raise ResultCardDeliveryConflict("result-card claim token is invalid")
    chat_id = marker.get("chat_id")
    if isinstance(chat_id, bool) or not isinstance(chat_id, int):
        raise ResultCardDeliveryConflict("result-card destination is invalid")
    text = marker.get("text")
    rich = isinstance(text, str) and bool(text.strip())
    if not rich:
        text = _fallback_text(marker)
    return marker, token, rich, text


async def deliver_result_card_once(bot, session_id: str, user_id: int | str) -> bool:
    """Attempt one due leased result-card delivery and durably settle its outcome."""
    claim = await asyncio.to_thread(claim_result_card_delivery, session_id, user_id)
    if claim is None:
        return False
    marker, token, rich, text = _claimed_payload(claim)

    async def sender():
        kwargs: dict[str, Any] = {
            "chat_id": marker["chat_id"],
            "text": text,
        }
        if rich:
            kwargs["parse_mode"] = "Markdown"
        return await bot.send_message(**kwargs)

    try:
        await send_with_durable_retry_after(sender)
    except LegacyDeliveryPermanentFailure as exc:
        settled = await asyncio.to_thread(
            settle_result_card_delivery_failure,
            session_id,
            user_id,
            token,
            error=exc.detail,
        )
        if not settled:
            raise ResultCardDeliveryAcknowledgementPending(
                "result-card permanent failure could not be durably settled"
            ) from exc
        return False
    except LegacyDeliveryDeferred as exc:
        deferred = await asyncio.to_thread(
            defer_result_card_delivery,
            session_id,
            user_id,
            token,
            delay_seconds=exc.delay_seconds,
            error=exc.detail or str(exc),
        )
        if not deferred:
            raise ResultCardDeliveryAcknowledgementPending(
                "result-card RetryAfter could not be durably deferred"
            ) from exc
        return False
    except Exception as exc:
        await asyncio.to_thread(
            release_result_card_delivery,
            session_id,
            user_id,
            token,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise

    acknowledged = await asyncio.to_thread(
        mark_result_card_delivered,
        session_id,
        user_id,
        token,
    )
    if not acknowledged:
        # Do not immediately release: the remote send completed. Lease expiry is
        # the safer retry boundary and matches the report/battle outbox policy.
        raise ResultCardDeliveryAcknowledgementPending(
            "result card was sent but durable acknowledgement is pending"
        )
    return True


async def deliver_live_result_card(
    bot,
    *,
    session_id: str,
    user_id: int | str,
    text: str,
) -> bool:
    """Persist the controller's exact rich card before its first remote send."""
    stored = await asyncio.to_thread(
        set_result_card_delivery_text,
        session_id,
        user_id,
        text,
    )
    if not stored:
        # The same marker is already terminally settled; suppress a duplicate
        # direct send from a replayed result renderer.
        return True
    await deliver_result_card_once(bot, session_id, user_id)
    return True


async def drain_result_card_outbox(bot, *, limit: int = 50) -> ResultCardDrainSummary:
    try:
        sessions = await asyncio.to_thread(get_pending_result_card_sessions, limit)
    except ResultCardDeliveryUnavailable as exc:
        return ResultCardDrainSummary(
            errors=(f"result-card-list:{type(exc).__name__}:{exc}"[:500],)
        )
    if not isinstance(sessions, list):
        raise ResultCardDeliveryConflict("pending result-card listing is invalid")

    delivered = 0
    deferred = 0
    errors: list[str] = []
    for session in sessions:
        session_id = session.get("_id") if isinstance(session, dict) else None
        user_id = session.get("user_id") if isinstance(session, dict) else None
        if not isinstance(session_id, str) or not session_id or user_id is None:
            errors.append("result-card:<invalid>:pending session identity is invalid")
            continue
        try:
            sent = await deliver_result_card_once(bot, session_id, user_id)
            if sent:
                delivered += 1
            else:
                deferred += 1
        except Exception as exc:
            errors.append(
                f"result-card:{session_id}:{type(exc).__name__}:{exc}"[:500]
            )
    return ResultCardDrainSummary(
        sessions_seen=len(sessions),
        delivered=delivered,
        deferred=deferred,
        errors=tuple(errors),
    )


async def result_card_delivery_job(context):
    try:
        summary = await drain_result_card_outbox(context.bot)
    except Exception:
        logger.exception("unexpected result-card outbox drain failure")
        return
    if summary.errors:
        logger.warning("result-card outbox drain completed with errors: %s", summary.errors)


class _ResultCardBotProxy:
    """Intercept only the first result-renderer send; pass later UI through."""

    def __init__(self, bot, *, session_id: str, user_id: int):
        self._bot = bot
        self._session_id = session_id
        self._user_id = user_id
        self._intercepted = False

    def __getattr__(self, name: str):
        return getattr(self._bot, name)

    async def send_message(self, *args, **kwargs):
        if self._intercepted:
            return await self._bot.send_message(*args, **kwargs)
        self._intercepted = True

        text = kwargs.get("text")
        if not isinstance(text, str) and len(args) >= 2:
            text = args[1]
        if not isinstance(text, str) or not text.strip():
            return await self._bot.send_message(*args, **kwargs)
        try:
            await deliver_live_result_card(
                self._bot,
                session_id=self._session_id,
                user_id=self._user_id,
                text=text,
            )
        except ResultCardDeliveryConflict as exc:
            # The only direct migration fallback is a genuinely absent marker
            # from a pre-deploy terminal session. Any contradictory outbox state
            # remains fail-closed so a later recovery job cannot duplicate a
            # conflicting direct send.
            if str(exc) != "result-card outbox marker is missing":
                raise
            logger.info(
                "pre-outbox result session %s has no marker; using legacy direct send",
                self._session_id,
            )
            return await self._bot.send_message(*args, **kwargs)
        return None


def install_result_card_renderer(quiz_module) -> bool:
    """Install one explicit composition-root wrapper around quiz._render_result."""
    renderer = getattr(quiz_module, "_render_result", None)
    if not callable(renderer):
        raise RuntimeError("quiz result renderer is unavailable")
    if getattr(renderer, "_durable_result_card_wrapper", False):
        return False

    async def wrapped(bot, user_id: int, outcome, *, retry_drill: bool = False):
        session_id = getattr(outcome, "session_id", None)
        if not isinstance(session_id, str) or not session_id:
            return await renderer(bot, user_id, outcome, retry_drill=retry_drill)
        proxy = _ResultCardBotProxy(bot, session_id=session_id, user_id=user_id)
        return await renderer(proxy, user_id, outcome, retry_drill=retry_drill)

    wrapped._durable_result_card_wrapper = True  # type: ignore[attr-defined]
    wrapped._wrapped_result_renderer = renderer  # type: ignore[attr-defined]
    quiz_module._render_result = wrapped
    return True
