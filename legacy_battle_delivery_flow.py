"""Immediate delivery orchestration for one retained finalized battle."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from battle_integrity import BATTLE_DELIVERY_PROTOCOL_OUTBOX
from legacy_delivery_worker import deliver_battle_recipient_once


class LegacyBattleDeliveryStateInvalid(RuntimeError):
    """Final battle snapshot cannot safely drive recipient delivery."""


@dataclass(frozen=True)
class BattleDeliveryOutcome:
    battle_id: str
    creator_sent: bool
    opponent_sent: bool
    creator_pending: bool
    opponent_pending: bool
    errors: tuple[str, ...] = ()


def _battle_identity(battle: dict) -> tuple[str, int, int]:
    if not isinstance(battle, dict):
        raise LegacyBattleDeliveryStateInvalid("final battle snapshot is invalid")
    battle_id = battle.get("_id")
    creator_id = battle.get("creator_id")
    opponent_id = battle.get("opponent_id")
    if not isinstance(battle_id, str) or not battle_id:
        raise LegacyBattleDeliveryStateInvalid("final battle id is invalid")
    for value, field in ((creator_id, "creator_id"), (opponent_id, "opponent_id")):
        if isinstance(value, bool) or not isinstance(value, int):
            raise LegacyBattleDeliveryStateInvalid(f"final battle {field} is invalid")
    if creator_id == opponent_id:
        raise LegacyBattleDeliveryStateInvalid("final battle participants are identical")
    if battle.get("final_claimed") is not True or battle.get("status") != "finalized":
        raise LegacyBattleDeliveryStateInvalid("battle is not a retained finalized result")
    if battle.get("result_delivery_protocol") != BATTLE_DELIVERY_PROTOCOL_OUTBOX:
        raise LegacyBattleDeliveryStateInvalid(
            "battle is not outbox-authoritative for result delivery"
        )
    return battle_id, creator_id, opponent_id


def _error(role: str, exc: Exception) -> str:
    return f"{role}:{type(exc).__name__}:{exc}"[:500]


async def deliver_final_battle_once(
    battle: dict,
    sender: Callable[[dict, str], Awaitable[Any]],
) -> BattleDeliveryOutcome:
    battle_id, creator_id, opponent_id = _battle_identity(battle)
    sent = {"creator": False, "opponent": False}
    pending = {"creator": False, "opponent": False}
    errors: list[str] = []
    for role, user_id in (("creator", creator_id), ("opponent", opponent_id)):
        try:
            delivered_now = await deliver_battle_recipient_once(battle_id, user_id, sender)
            sent[role] = delivered_now
            pending[role] = not delivered_now
        except Exception as exc:
            pending[role] = True
            errors.append(_error(role, exc))
    return BattleDeliveryOutcome(
        battle_id=battle_id,
        creator_sent=sent["creator"], opponent_sent=sent["opponent"],
        creator_pending=pending["creator"], opponent_pending=pending["opponent"],
        errors=tuple(errors),
    )
