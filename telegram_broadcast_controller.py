"""Production adapter for restart-safe administrator broadcasts."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta

from pymongo.errors import PyMongoError
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut

import bot as legacy
from broadcast_integrity import (
    BroadcastStoreUnavailable,
    accept_broadcast_once,
    broadcast_id_for_update,
    claim_next_broadcast_delivery,
    defer_broadcast_delivery,
    ensure_broadcast_fanout,
    get_broadcast,
    get_pending_broadcasts,
    mark_broadcast_delivery_delivered,
    mark_broadcast_delivery_terminal_failure,
    release_broadcast_delivery,
    sync_broadcast_completion,
)
from config import BROADCAST_SLEEP

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BroadcastDrainSummary:
    claimed: int = 0
    delivered: int = 0
    terminal_failed: int = 0
    deferred: int = 0
    errors: tuple[str, ...] = ()


def _recipient_ids_strict() -> list[int]:
    """Read the complete recipient snapshot without legacy empty-on-error fallback."""
    import database

    users = getattr(database, "collection", None)
    if users is None:
        raise BroadcastStoreUnavailable("broadcast recipient storage is unavailable")
    try:
        recipients = []
        for doc in users.find({}, {"_id": 1}):
            raw = doc.get("_id") if isinstance(doc, dict) else None
            if isinstance(raw, bool):
                raise BroadcastStoreUnavailable("broadcast recipient id is invalid")
            recipients.append(int(raw))
        return recipients
    except BroadcastStoreUnavailable:
        raise
    except (PyMongoError, TypeError, ValueError) as exc:
        raise BroadcastStoreUnavailable("broadcast recipient snapshot failed") from exc


def _replay_broadcast(
    stored: dict,
    *,
    admin_id: int,
    admin_chat_id: int,
    text: str,
) -> tuple[dict, list[str]]:
    """Validate one already accepted immutable command without resnapshotting users."""
    if (
        stored.get("admin_id") != str(admin_id)
        or stored.get("admin_chat_id") != str(admin_chat_id)
        or stored.get("text") != text
    ):
        raise BroadcastStoreUnavailable(
            "broadcast update id is bound to different immutable content"
        )
    recipients = stored.get("recipient_ids")
    if not isinstance(recipients, list) or not all(
        isinstance(value, str) and value.isdigit() and int(value) > 0
        for value in recipients
    ):
        raise BroadcastStoreUnavailable("durable broadcast recipient snapshot is invalid")
    return stored, recipients


def _accept_or_recover_new_broadcast(
    *,
    broadcast_id: str,
    admin_id: int,
    admin_chat_id: int,
    text: str,
) -> tuple[dict, bool, list[int] | list[str]]:
    """Accept once, then read back deterministic id after an ambiguous write error."""
    recipients = _recipient_ids_strict()
    try:
        stored, created = accept_broadcast_once(
            broadcast_id=broadcast_id,
            admin_id=admin_id,
            admin_chat_id=admin_chat_id,
            text=text,
            recipient_ids=recipients,
        )
        return stored, created, recipients
    except BroadcastStoreUnavailable:
        recovered = get_broadcast(broadcast_id)
        if not isinstance(recovered, dict):
            raise
        stored, persisted_recipients = _replay_broadcast(
            recovered,
            admin_id=admin_id,
            admin_chat_id=admin_chat_id,
            text=text,
        )
        return stored, False, persisted_recipients


def _retry_after_seconds(exc: RetryAfter) -> float:
    value = exc.retry_after
    if isinstance(value, timedelta):
        return max(1.0, value.total_seconds())
    try:
        return max(1.0, float(value))
    except (TypeError, ValueError):
        return 1.0


def _broadcast_text(text: str) -> str:
    return f"📢 Сообщение от автора бота:\n\n{text}"


async def drain_broadcast_outbox(
    bot,
    *,
    limit: int = 20,
    broadcast_id: str | None = None,
) -> BroadcastDrainSummary:
    """Drain a bounded broadcast batch while isolating per-recipient failures."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")

    errors: list[str] = []
    affected: set[str] = set()
    try:
        if broadcast_id is not None:
            parent = get_broadcast(broadcast_id)
            parents = [parent] if isinstance(parent, dict) else []
        else:
            parents = get_pending_broadcasts(limit=20)
        for parent in parents:
            if not isinstance(parent, dict):
                continue
            parent_id = parent.get("_id") or parent.get("broadcast_id")
            if not isinstance(parent_id, str) or not parent_id:
                errors.append("broadcast:<unknown>:invalid parent id")
                continue
            affected.add(parent_id)
            if parent.get("fanout_ready") is not True:
                ensure_broadcast_fanout(parent)
    except (BroadcastStoreUnavailable, ValueError) as exc:
        return BroadcastDrainSummary(errors=(f"broadcast-prepare:{type(exc).__name__}:{exc}"[:500],))

    claimed_count = 0
    delivered_count = 0
    terminal_failed = 0
    deferred = 0

    for _ in range(limit):
        try:
            delivery = claim_next_broadcast_delivery(broadcast_id=broadcast_id)
        except BroadcastStoreUnavailable as exc:
            errors.append(f"broadcast-claim:{type(exc).__name__}:{exc}"[:500])
            break
        if delivery is None:
            break
        claimed_count += 1

        delivery_id = delivery.get("_id")
        parent_id = delivery.get("broadcast_id")
        claim_token = delivery.get("claim_token")
        raw_user_id = delivery.get("user_id")
        if not all(isinstance(value, str) and value for value in (delivery_id, parent_id, claim_token)):
            errors.append("broadcast-delivery:<invalid>:malformed claimed row")
            break
        affected.add(parent_id)
        try:
            if not isinstance(raw_user_id, str) or not raw_user_id.isdigit():
                if mark_broadcast_delivery_terminal_failure(
                    delivery_id,
                    claim_token,
                    error="broadcast recipient id is invalid",
                ):
                    terminal_failed += 1
                continue
            user_id = int(raw_user_id)
            if user_id <= 0:
                if mark_broadcast_delivery_terminal_failure(
                    delivery_id,
                    claim_token,
                    error="broadcast recipient id is invalid",
                ):
                    terminal_failed += 1
                continue

            parent = get_broadcast(parent_id)
            if not isinstance(parent, dict):
                if mark_broadcast_delivery_terminal_failure(
                    delivery_id,
                    claim_token,
                    error="broadcast parent is missing",
                ):
                    terminal_failed += 1
                continue
            text = parent.get("text")
            if not isinstance(text, str) or not text:
                if mark_broadcast_delivery_terminal_failure(
                    delivery_id,
                    claim_token,
                    error="broadcast text is invalid",
                ):
                    terminal_failed += 1
                continue

            try:
                await bot.send_message(chat_id=user_id, text=_broadcast_text(text))
            except (Forbidden, BadRequest) as exc:
                if mark_broadcast_delivery_terminal_failure(
                    delivery_id,
                    claim_token,
                    error=f"{type(exc).__name__}: {exc}",
                ):
                    terminal_failed += 1
            except RetryAfter as exc:
                delay = _retry_after_seconds(exc)
                if not defer_broadcast_delivery(
                    delivery_id,
                    claim_token,
                    delay_seconds=delay,
                    error=f"{type(exc).__name__}: {exc}",
                ):
                    errors.append(f"broadcast:{parent_id}:{delivery_id}:defer conflict")
                deferred += 1
                break
            except (NetworkError, TimedOut) as exc:
                release_broadcast_delivery(
                    delivery_id,
                    claim_token,
                    error=f"{type(exc).__name__}: {exc}",
                )
                deferred += 1
                break
            except Exception as exc:
                release_broadcast_delivery(
                    delivery_id,
                    claim_token,
                    error=f"{type(exc).__name__}: {exc}",
                )
                deferred += 1
                errors.append(
                    f"broadcast:{parent_id}:{delivery_id}:{type(exc).__name__}:{exc}"[:500]
                )
                break
            else:
                if mark_broadcast_delivery_delivered(delivery_id, claim_token):
                    delivered_count += 1
                else:
                    errors.append(f"broadcast:{parent_id}:{delivery_id}:ack conflict")
            await asyncio.sleep(max(0.0, float(BROADCAST_SLEEP)))
        except (BroadcastStoreUnavailable, TypeError, ValueError) as exc:
            errors.append(
                f"broadcast:{parent_id}:{delivery_id}:{type(exc).__name__}:{exc}"[:500]
            )
            try:
                release_broadcast_delivery(
                    delivery_id,
                    claim_token,
                    error=f"{type(exc).__name__}: {exc}",
                )
            except Exception:
                pass
            deferred += 1
            break

    for parent_id in sorted(affected):
        try:
            sync_broadcast_completion(parent_id)
        except (BroadcastStoreUnavailable, ValueError) as exc:
            errors.append(f"broadcast-sync:{parent_id}:{type(exc).__name__}:{exc}"[:500])

    return BroadcastDrainSummary(
        claimed=claimed_count,
        delivered=delivered_count,
        terminal_failed=terminal_failed,
        deferred=deferred,
        errors=tuple(errors),
    )


async def broadcast_delivery_job(context) -> None:
    try:
        summary = await drain_broadcast_outbox(context.bot, limit=20)
        if summary.errors:
            logger.warning("broadcast outbox drain completed with errors: %s", summary.errors)
    except Exception:
        logger.exception("unexpected broadcast outbox drain failure")


async def broadcast_command(update, context):
    """Durably accept `/broadcast text`; delivery runs outside the update handler."""
    del context
    user = update.effective_user
    message = update.message
    if user is None or message is None:
        return
    if user.id != legacy.ADMIN_USER_ID:
        await message.reply_text("❌ Нет доступа.")
        return

    raw_text = message.text or ""
    text = raw_text.replace("/broadcast", "", 1).strip()
    if not text:
        await message.reply_text("Использование: `/broadcast Текст сообщения`", parse_mode="Markdown")
        return

    try:
        broadcast_id = broadcast_id_for_update(update.update_id)
        existing = get_broadcast(broadcast_id)
        if isinstance(existing, dict):
            stored, recipients = _replay_broadcast(
                existing,
                admin_id=user.id,
                admin_chat_id=message.chat_id,
                text=text,
            )
            created = False
        else:
            stored, created, recipients = _accept_or_recover_new_broadcast(
                broadcast_id=broadcast_id,
                admin_id=user.id,
                admin_chat_id=message.chat_id,
                text=text,
            )
    except (BroadcastStoreUnavailable, ValueError):
        logger.warning("durable broadcast acceptance failed for admin %s", user.id, exc_info=True)
        await message.reply_text(
            "Broadcast status is unknown. Do not create a new command; retry this command later."
        )
        return

    count = int(stored.get("recipient_count", len(recipients)) or 0)
    if created:
        await message.reply_text(
            f"✅ Рассылка сохранена в durable-очередь: {count} получателей. "
            "Доставка продолжится автоматически после перезапуска."
        )
    else:
        await message.reply_text(
            f"Эта команда уже принята: {count} получателей. "
            "Новая очередь доставки не добавлена."
        )
