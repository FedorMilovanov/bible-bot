"""Async orchestration over durable battle/report delivery leases.

The module knows nothing about Telegram handlers or application lifecycle. A
caller injects the actual send coroutine. Storage decides ownership, lease and
acknowledgement; sender failures release the lease for retry. If delivery was
accepted remotely but Mongo acknowledgement later fails, the lease is left to
expire rather than being released immediately, reducing duplicate-send risk.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from battle_integrity import (
    claim_battle_result_delivery,
    mark_battle_result_delivered,
    release_battle_result_delivery,
)
from report_integrity import (
    claim_report_delivery_stage,
    mark_report_delivery_stage_delivered,
    release_report_delivery_stage,
)


class LegacyDeliveryStateInvalid(RuntimeError):
    """Durable outbox state is missing data required by a sender."""


class LegacyDeliveryAcknowledgementPending(RuntimeError):
    """Remote send completed but durable acknowledgement did not."""


async def deliver_battle_recipient_once(
    battle_id: str,
    user_id: int,
    sender: Callable[[dict, str], Awaitable[Any]],
) -> bool:
    """Attempt one leased battle-result delivery for one participant."""
    claim = claim_battle_result_delivery(battle_id, user_id)
    if claim is None:
        return False
    battle = claim.get("battle")
    role = claim.get("role")
    token = claim.get("claim_token")
    if not isinstance(battle, dict) or role not in {"creator", "opponent"}:
        raise LegacyDeliveryStateInvalid("battle delivery claim is malformed")
    if not isinstance(token, str) or not token:
        raise LegacyDeliveryStateInvalid("battle delivery claim token is missing")

    try:
        await sender(battle, role)
    except Exception as exc:
        release_battle_result_delivery(
            battle_id,
            user_id,
            token,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise

    if not mark_battle_result_delivered(battle_id, user_id, token):
        raise LegacyDeliveryAcknowledgementPending(
            "battle result was sent but acknowledgement is pending"
        )
    return True


async def _deliver_report_stage_once(
    report_id: str,
    stage: str,
    sender: Callable[[dict], Awaitable[Any]],
) -> bool:
    claim = claim_report_delivery_stage(report_id, stage)
    if claim is None:
        return False
    report = claim.get("report")
    token = claim.get("claim_token")
    if not isinstance(report, dict):
        raise LegacyDeliveryStateInvalid("report delivery claim is malformed")
    if not isinstance(token, str) or not token:
        raise LegacyDeliveryStateInvalid("report delivery claim token is missing")
    if stage == "photo" and not report.get("photo_file_id"):
        release_report_delivery_stage(
            report_id,
            stage,
            token,
            error="photo delivery stage has no durable photo_file_id",
        )
        raise LegacyDeliveryStateInvalid(
            "photo delivery stage has no durable photo_file_id"
        )

    try:
        await sender(report)
    except Exception as exc:
        release_report_delivery_stage(
            report_id,
            stage,
            token,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise

    if not mark_report_delivery_stage_delivered(report_id, stage, token):
        raise LegacyDeliveryAcknowledgementPending(
            f"report {stage} was sent but acknowledgement is pending"
        )
    return True


async def deliver_report_once(
    report_id: str,
    photo_sender: Callable[[dict], Awaitable[Any]],
    text_sender: Callable[[dict], Awaitable[Any]],
) -> tuple[bool, bool]:
    """Deliver pending photo then text stages without replaying acknowledged work."""
    photo = await _deliver_report_stage_once(report_id, "photo", photo_sender)
    text = await _deliver_report_stage_once(report_id, "text", text_sender)
    return photo, text
