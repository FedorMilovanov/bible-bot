"""Settle permanent failures in existing report/battle delivery contracts.

The historical `delivered` fields are also the pending/retention guard used by
production and existing partial TTL indexes. A permanent remote rejection has no
remaining delivery obligation, so this module settles that guard to True while
recording `terminal_failed=True`, the bounded error text and terminal timestamp.
Consumers can therefore distinguish a successful send from a permanent failure
without a destructive index migration or an infinite retry loop.
"""
from __future__ import annotations

from pymongo.errors import PyMongoError

from battle_integrity import (
    BATTLE_DELIVERY_PROTOCOL_OUTBOX,
    BattleStoreUnavailable,
    battle_role_for_user,
)
from report_integrity import ReportStoreUnavailable

_REPORT_STAGES = frozenset({"photo", "text"})


def settle_report_delivery_stage_failure(
    report_id: str,
    stage: str,
    claim_token: str,
    *,
    error: str,
) -> bool:
    if not isinstance(report_id, str) or not report_id.strip():
        raise ValueError("report_id is required")
    if stage not in _REPORT_STAGES:
        raise ValueError("unsupported report delivery stage")
    if not isinstance(claim_token, str) or not claim_token:
        raise ValueError("claim_token is required")

    import database

    reports = getattr(database, "reports_collection", None)
    if reports is None:
        raise ReportStoreUnavailable("report storage is unavailable")
    now = database._now_utc()
    path = f"delivery.{stage}"
    try:
        result = reports.update_one(
            {
                "_id": report_id,
                f"{path}.delivered": {"$ne": True},
                f"{path}.claim_token": claim_token,
            },
            {
                "$set": {
                    f"{path}.delivered": True,
                    f"{path}.terminal_failed": True,
                    f"{path}.terminal_error": str(error or "")[:500],
                    f"{path}.terminal_at": now,
                },
                "$unset": {
                    f"{path}.claim_token": "",
                    f"{path}.lease_until": "",
                    f"{path}.last_error": "",
                },
            },
        )
        if result.modified_count != 1:
            existing = reports.find_one(
                {"_id": report_id},
                {f"{path}.delivered": 1, f"{path}.terminal_failed": 1},
            )
            delivery = existing.get("delivery", {}) if isinstance(existing, dict) else {}
            state = delivery.get(stage, {}) if isinstance(delivery, dict) else {}
            if not (
                isinstance(state, dict)
                and state.get("delivered") is True
                and state.get("terminal_failed") is True
            ):
                return False

        current = reports.find_one(
            {"_id": report_id},
            {
                "delivery.photo.delivered": 1,
                "delivery.photo.terminal_failed": 1,
                "delivery.text.delivered": 1,
                "delivery.text.terminal_failed": 1,
                "admin_delivered": 1,
            },
        )
        if not isinstance(current, dict):
            raise ReportStoreUnavailable("report disappeared during terminal settlement")
        delivery = current.get("delivery")
        photo = delivery.get("photo") if isinstance(delivery, dict) else None
        text = delivery.get("text") if isinstance(delivery, dict) else None
        if not isinstance(photo, dict) or not isinstance(text, dict):
            raise ReportStoreUnavailable("report delivery state is malformed")
        if photo.get("delivered") is True and text.get("delivered") is True:
            update = {
                "$set": {
                    "admin_delivered": True,
                    "admin_delivered_at": now,
                }
            }
            if photo.get("terminal_failed") is True or text.get("terminal_failed") is True:
                update["$set"]["admin_delivery_failed"] = True
            reports.update_one(
                {"_id": report_id, "admin_delivered": {"$ne": True}},
                update,
            )
        return True
    except ReportStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise ReportStoreUnavailable("report permanent delivery settlement failed") from exc


def settle_battle_result_delivery_failure(
    battle_id: str,
    user_id: int,
    claim_token: str,
    *,
    error: str,
) -> bool:
    if not isinstance(battle_id, str) or not battle_id.strip():
        raise ValueError("battle_id is required")
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id must be a positive integer")
    if not isinstance(claim_token, str) or not claim_token:
        raise ValueError("claim_token is required")

    import database

    battles = getattr(database, "battles_collection", None)
    if battles is None:
        raise BattleStoreUnavailable("battle collection is unavailable")
    try:
        battle = battles.find_one(
            {
                "_id": battle_id,
                "final_claimed": True,
                "result_delivery_protocol": BATTLE_DELIVERY_PROTOCOL_OUTBOX,
            }
        )
        role = battle_role_for_user(battle, user_id)
        if role is None:
            return False
        now = database._now_utc()
        path = f"result_delivery.{role}"
        result = battles.update_one(
            {
                "_id": battle_id,
                "result_delivery_protocol": BATTLE_DELIVERY_PROTOCOL_OUTBOX,
                f"{path}.delivered": {"$ne": True},
                f"{path}.claim_token": claim_token,
            },
            {
                "$set": {
                    f"{path}.delivered": True,
                    f"{path}.terminal_failed": True,
                    f"{path}.terminal_error": str(error or "")[:500],
                    f"{path}.terminal_at": now,
                },
                "$unset": {
                    f"{path}.claim_token": "",
                    f"{path}.lease_until": "",
                    f"{path}.last_error": "",
                },
            },
        )
        if result.modified_count == 1:
            return True
        existing = battles.find_one(
            {
                "_id": battle_id,
                "result_delivery_protocol": BATTLE_DELIVERY_PROTOCOL_OUTBOX,
            },
            {f"{path}.delivered": 1, f"{path}.terminal_failed": 1},
        )
        delivered = existing or {}
        for part in path.split("."):
            delivered = delivered.get(part, {}) if isinstance(delivered, dict) else {}
        return (
            isinstance(delivered, dict)
            and delivered.get("delivered") is True
            and delivered.get("terminal_failed") is True
        )
    except PyMongoError as exc:
        raise BattleStoreUnavailable("battle permanent delivery settlement failed") from exc
