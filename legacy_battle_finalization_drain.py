"""Recovery sweep for durable PvP battles whose participant results are complete."""
from __future__ import annotations

from dataclasses import dataclass

from pymongo.errors import PyMongoError

from battle_integrity import (
    BATTLE_DELIVERY_PROTOCOL_OUTBOX,
    BattleStoreUnavailable,
    claim_final_battle,
)
from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE


class LegacyBattleFinalizationQueueUnavailable(RuntimeError):
    """Ready-battle discovery cannot reach MongoDB."""


@dataclass(frozen=True)
class BattleFinalizationDrainSummary:
    battles_seen: int = 0
    finalized: int = 0
    deferred: int = 0
    errors: tuple[str, ...] = ()


def _collection():
    import database

    collection = getattr(database, "battles_collection", None)
    if collection is None:
        raise LegacyBattleFinalizationQueueUnavailable("battle collection is unavailable")
    return collection


def _ready_battle_ids(limit: int) -> list[str]:
    try:
        rows = list(
            _collection().find(
                {
                    "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
                    "creator_finished": True,
                    "opponent_finished": True,
                    "final_claimed": {"$ne": True},
                    "status": {"$in": ["waiting", "in_progress"]},
                },
                {"_id": 1},
            ).limit(limit)
        )
    except PyMongoError as exc:
        raise LegacyBattleFinalizationQueueUnavailable(
            "ready battle finalization lookup failed"
        ) from exc
    ids = []
    for row in rows:
        battle_id = row.get("_id") if isinstance(row, dict) else None
        if not isinstance(battle_id, str) or not battle_id:
            raise LegacyBattleFinalizationQueueUnavailable(
                "ready battle finalization id is invalid"
            )
        ids.append(battle_id)
    return ids


def finalize_ready_battles(*, limit: int = 50) -> BattleFinalizationDrainSummary:
    """Finalize all discovered durable-v1 ready battles with outbox-v1 delivery."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    try:
        battle_ids = _ready_battle_ids(limit)
    except LegacyBattleFinalizationQueueUnavailable as exc:
        return BattleFinalizationDrainSummary(
            errors=(f"battle-finalize-list:{type(exc).__name__}:{exc}"[:500],)
        )

    finalized = 0
    deferred = 0
    errors: list[str] = []
    for battle_id in battle_ids:
        try:
            result = claim_final_battle(
                battle_id,
                delivery_protocol=BATTLE_DELIVERY_PROTOCOL_OUTBOX,
            )
            if result is None:
                deferred += 1
            else:
                finalized += 1
        except BattleStoreUnavailable as exc:
            errors.append(
                f"battle-finalize:{battle_id}:{type(exc).__name__}:{exc}"[:500]
            )
    return BattleFinalizationDrainSummary(
        battles_seen=len(battle_ids),
        finalized=finalized,
        deferred=deferred,
        errors=tuple(errors),
    )
