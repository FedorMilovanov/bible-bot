"""Durable defer primitives for existing report and battle delivery leases.

These operations do not create a second outbox protocol. They only extend the
lease of an already claimed report stage or battle recipient after an upstream
sender explicitly asks for a future retry (for example Telegram RetryAfter).
The claim token is removed while the future lease remains, so another process
cannot reclaim the delivery before the requested time.
"""
from __future__ import annotations

import math
from datetime import timedelta

from pymongo.errors import PyMongoError

from battle_integrity import (
    BATTLE_DELIVERY_PROTOCOL_OUTBOX,
    BattleStoreUnavailable,
    battle_role_for_user,
)
from report_integrity import ReportStoreUnavailable

_REPORT_STAGES = frozenset({"photo", "text"})


def _delay_seconds(value) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("delay_seconds must be a positive finite number")
    delay = float(value)
    if not math.isfinite(delay) or delay <= 0:
        raise ValueError("delay_seconds must be a positive finite number")
    return delay


def defer_report_delivery_stage(
    report_id: str,
    stage: str,
    claim_token: str,
    *,
    delay_seconds: float,
    error: str = "",
) -> bool:
    if not isinstance(report_id, str) or not report_id.strip():
        raise ValueError("report_id is required")
    if stage not in _REPORT_STAGES:
        raise ValueError("unsupported report delivery stage")
    if not isinstance(claim_token, str) or not claim_token:
        raise ValueError("claim_token is required")
    delay = _delay_seconds(delay_seconds)

    import database

    reports = getattr(database, "reports_collection", None)
    if reports is None:
        raise ReportStoreUnavailable("report storage is unavailable")
    now = database._now_utc()
    try:
        lease_until = now + timedelta(seconds=delay)
    except OverflowError as exc:
        raise ValueError("delay_seconds is too large") from exc
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
                    f"{path}.lease_until": lease_until,
                    f"{path}.last_error": str(error or "")[:500],
                },
                "$unset": {f"{path}.claim_token": ""},
            },
        )
        return result.modified_count == 1
    except PyMongoError as exc:
        raise ReportStoreUnavailable("report delivery deferral failed") from exc


def defer_battle_result_delivery(
    battle_id: str,
    user_id: int,
    claim_token: str,
    *,
    delay_seconds: float,
    error: str = "",
) -> bool:
    if not isinstance(battle_id, str) or not battle_id.strip():
        raise ValueError("battle_id is required")
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id must be a positive integer")
    if not isinstance(claim_token, str) or not claim_token:
        raise ValueError("claim_token is required")
    delay = _delay_seconds(delay_seconds)

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
        try:
            lease_until = now + timedelta(seconds=delay)
        except OverflowError as exc:
            raise ValueError("delay_seconds is too large") from exc
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
                    f"{path}.lease_until": lease_until,
                    f"{path}.last_error": str(error or "")[:500],
                },
                "$unset": {f"{path}.claim_token": ""},
            },
        )
        return result.modified_count == 1
    except ValueError:
        raise
    except PyMongoError as exc:
        raise BattleStoreUnavailable("battle result delivery deferral failed") from exc
