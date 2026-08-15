# ruff: noqa: RUF001
"""Telegram adapter for crash-safe creator-ready battle notifications."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass

from legacy_battle_ready_delivery import (
    LegacyBattleReadyDeliveryConflict,
    LegacyBattleReadyDeliveryUnavailable,
    claim_creator_ready_delivery,
    defer_creator_ready_delivery,
    get_pending_creator_ready_battles,
    mark_creator_ready_delivered,
    release_creator_ready_delivery,
    settle_creator_ready_failure,
)
from legacy_delivery_worker import LegacyDeliveryDeferred, LegacyDeliveryPermanentFailure
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram_delivery_retry import send_with_durable_retry_after

logger = logging.getLogger(__name__)


class BattleReadyDeliveryAcknowledgementPending(RuntimeError):
    """Remote send may have completed while Mongo acknowledgement is unresolved."""


@dataclass(frozen=True)
class BattleReadyDrainSummary:
    battles_seen: int = 0
    delivered: int = 0
    deferred: int = 0
    errors: tuple[str, ...] = ()


def _claimed_payload(claim: dict, start_payload_builder: Callable[[str, str], str]):
    if not isinstance(claim, dict):
        raise LegacyBattleReadyDeliveryConflict("battle-ready claim is invalid")
    battle = claim.get("battle")
    token = claim.get("claim_token")
    if not isinstance(battle, dict):
        raise LegacyBattleReadyDeliveryConflict("battle-ready claimed battle is invalid")
    battle_id = battle.get("_id")
    creator_id = battle.get("creator_id")
    opponent_name = battle.get("opponent_name")
    if not isinstance(battle_id, str) or not battle_id:
        raise LegacyBattleReadyDeliveryConflict("battle-ready id is invalid")
    if isinstance(creator_id, bool) or not isinstance(creator_id, int) or creator_id <= 0:
        raise LegacyBattleReadyDeliveryConflict("battle-ready creator id is invalid")
    if not isinstance(opponent_name, str) or not opponent_name.strip():
        raise LegacyBattleReadyDeliveryConflict("battle-ready opponent name is invalid")
    if not isinstance(token, str) or not token:
        raise LegacyBattleReadyDeliveryConflict("battle-ready claim token is invalid")
    callback_data = start_payload_builder(battle_id, "creator")
    if not isinstance(callback_data, str) or not callback_data:
        raise LegacyBattleReadyDeliveryConflict("battle-ready callback is invalid")
    return battle_id, creator_id, opponent_name.strip()[:128], token, callback_data


async def deliver_creator_ready_once(
    bot,
    battle_id: str,
    *,
    start_payload_builder: Callable[[str, str], str],
) -> bool:
    """Attempt one leased creator-ready notification and durably settle it."""
    claim = await asyncio.to_thread(claim_creator_ready_delivery, battle_id)
    if claim is None:
        return False
    battle_id, creator_id, opponent_name, token, callback_data = _claimed_payload(
        claim,
        start_payload_builder,
    )

    async def sender():
        return await bot.send_message(
            chat_id=creator_id,
            text=f"⚔️ *Соперник найден:* {opponent_name}\nМожно начинать битву.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("▶️ Начать", callback_data=callback_data)]]
            ),
            parse_mode="Markdown",
        )

    try:
        await send_with_durable_retry_after(sender)
    except LegacyDeliveryPermanentFailure as exc:
        settled = await asyncio.to_thread(
            settle_creator_ready_failure,
            battle_id,
            token,
            error=exc.detail,
        )
        if not settled:
            raise BattleReadyDeliveryAcknowledgementPending(
                "battle-ready permanent failure could not be durably settled"
            ) from exc
        return False
    except LegacyDeliveryDeferred as exc:
        deferred = await asyncio.to_thread(
            defer_creator_ready_delivery,
            battle_id,
            token,
            delay_seconds=exc.delay_seconds,
            error=exc.detail or str(exc),
        )
        if not deferred:
            raise BattleReadyDeliveryAcknowledgementPending(
                "battle-ready RetryAfter could not be durably deferred"
            ) from exc
        return False
    except Exception as exc:
        released = await asyncio.to_thread(
            release_creator_ready_delivery,
            battle_id,
            token,
            error=f"{type(exc).__name__}: {exc}",
        )
        if not released:
            logger.warning("battle-ready transient failure lease release was not confirmed")
        raise

    acknowledged = await asyncio.to_thread(
        mark_creator_ready_delivered,
        battle_id,
        token,
    )
    if not acknowledged:
        # Remote delivery completed. Keep the lease until expiry instead of
        # immediately retrying and increasing duplicate risk.
        raise BattleReadyDeliveryAcknowledgementPending(
            "battle-ready notification was sent but acknowledgement is pending"
        )
    return True


async def drain_creator_ready_outbox(
    bot,
    *,
    start_payload_builder: Callable[[str, str], str],
    limit: int = 50,
) -> BattleReadyDrainSummary:
    try:
        battles = await asyncio.to_thread(get_pending_creator_ready_battles, limit)
    except LegacyBattleReadyDeliveryUnavailable as exc:
        return BattleReadyDrainSummary(
            errors=(f"battle-ready-list:{type(exc).__name__}:{exc}"[:500],)
        )
    if not isinstance(battles, list):
        raise LegacyBattleReadyDeliveryConflict("pending battle-ready listing is invalid")

    delivered = 0
    deferred = 0
    errors: list[str] = []
    for battle in battles:
        battle_id = battle.get("_id") if isinstance(battle, dict) else None
        if not isinstance(battle_id, str) or not battle_id:
            errors.append("battle-ready:<invalid>:pending battle identity is invalid")
            continue
        try:
            sent = await deliver_creator_ready_once(
                bot,
                battle_id,
                start_payload_builder=start_payload_builder,
            )
            if sent:
                delivered += 1
            else:
                deferred += 1
        except Exception as exc:
            errors.append(f"battle-ready:{battle_id}:{type(exc).__name__}:{exc}"[:500])
    return BattleReadyDrainSummary(
        battles_seen=len(battles),
        delivered=delivered,
        deferred=deferred,
        errors=tuple(errors),
    )
