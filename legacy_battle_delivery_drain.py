"""Battle-only durable outbox drain for production Telegram PvP results."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from battle_integrity import BattleStoreUnavailable, get_pending_final_battles
from legacy_battle_delivery_flow import deliver_final_battle_once
from telegram_delivery_retry import send_with_durable_retry_after


class LegacyBattleDeliveryQueueInvalid(RuntimeError):
    """Pending finalized battle evidence is structurally invalid."""


@dataclass(frozen=True)
class BattleDeliveryDrainSummary:
    battles_seen: int = 0
    recipient_sends: int = 0
    deferred: int = 0
    errors: tuple[str, ...] = ()


def _battle_id(battle: dict) -> str:
    value = battle.get("_id") if isinstance(battle, dict) else None
    if not isinstance(value, str) or not value:
        raise LegacyBattleDeliveryQueueInvalid("pending final battle id is invalid")
    return value


def _error(identifier: str, exc: Exception) -> str:
    return f"battle:{identifier}:{type(exc).__name__}:{exc}"[:500]


async def drain_pending_battles(
    *,
    sender: Callable[[dict, str], Awaitable[Any]],
    limit: int = 50,
) -> BattleDeliveryDrainSummary:
    """Drain retained outbox-v1 battle results with per-battle failure isolation."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    try:
        battles = get_pending_final_battles(limit)
    except BattleStoreUnavailable as exc:
        return BattleDeliveryDrainSummary(
            errors=(f"battle-list:<queue>:{type(exc).__name__}:{exc}"[:500],)
        )
    if not isinstance(battles, list):
        raise LegacyBattleDeliveryQueueInvalid(
            "pending finalized battle listing returned invalid data"
        )

    async def durable_sender(battle: dict, role: str):
        return await send_with_durable_retry_after(sender, battle, role)

    sends = 0
    deferred = 0
    errors: list[str] = []
    for battle in battles:
        identifier = "<unknown>"
        try:
            identifier = _battle_id(battle)
            outcome = await deliver_final_battle_once(battle, durable_sender)
            sends += int(outcome.creator_sent) + int(outcome.opponent_sent)
            deferred += int(outcome.creator_pending) + int(outcome.opponent_pending)
            errors.extend(
                f"battle:{identifier}:{item}"[:500]
                for item in outcome.errors
            )
        except Exception as exc:
            errors.append(_error(identifier, exc))
    return BattleDeliveryDrainSummary(
        battles_seen=len(battles),
        recipient_sends=sends,
        deferred=deferred,
        errors=tuple(errors),
    )
