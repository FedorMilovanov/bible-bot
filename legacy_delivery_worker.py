"""Async orchestration over durable battle/report delivery leases.

The module knows nothing about Telegram handlers or application lifecycle. A
caller injects the actual send coroutine. Storage decides ownership, lease and
acknowledgement. Ordinary sender failures release the lease for retry; an
explicit LegacyDeliveryDeferred signal preserves the requested future retry in
Mongo without keeping a process-local claim alive. If delivery was accepted
remotely but Mongo acknowledgement later fails, the lease is left to expire
rather than being released immediately, reducing duplicate-send risk.
"""
from __future__ import annotations

import math
from collections.abc import Awaitable, Callable
from typing import Any

from battle_integrity import (
    claim_battle_result_delivery,
    mark_battle_result_delivered,
    release_battle_result_delivery,
)
from legacy_delivery_defer import (
    defer_battle_result_delivery,
    defer_report_delivery_stage,
)
from legacy_report_delivery_migration import (
    claim_report_delivery_stage_compatible as claim_report_delivery_stage,
)
from report_integrity import (
    get_report_delivery_stage_state,
    mark_report_delivery_stage_delivered,
    release_report_delivery_stage,
)


class LegacyDeliveryStateInvalid(RuntimeError):
    """Durable outbox state is missing data required by a sender."""


class LegacyDeliveryAcknowledgementPending(RuntimeError):
    """Remote send completed but durable acknowledgement did not."""


class LegacyDeliveryDeferred(RuntimeError):
    """Sender requests a durable future retry instead of immediate release."""

    def __init__(self, delay_seconds: float, detail: str = ""):
        if isinstance(delay_seconds, bool) or not isinstance(delay_seconds, (int, float)):
            raise ValueError("delay_seconds must be a positive finite number")
        delay = float(delay_seconds)
        if not math.isfinite(delay) or delay <= 0:
            raise ValueError("delay_seconds must be a positive finite number")
        self.delay_seconds = delay
        self.detail = str(detail or "")[:500]
        super().__init__(self.detail or f"delivery deferred for {delay:g} seconds")


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
    except LegacyDeliveryDeferred as exc:
        if not defer_battle_result_delivery(
            battle_id,
            user_id,
            token,
            delay_seconds=exc.delay_seconds,
            error=exc.detail or str(exc),
        ):
            raise LegacyDeliveryAcknowledgementPending(
                "battle result deferral could not be acknowledged"
            ) from exc
        return False
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
    except LegacyDeliveryDeferred as exc:
        if not defer_report_delivery_stage(
            report_id,
            stage,
            token,
            delay_seconds=exc.delay_seconds,
            error=exc.detail or str(exc),
        ):
            raise LegacyDeliveryAcknowledgementPending(
                f"report {stage} deferral could not be acknowledged"
            ) from exc
        return False
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
    """Deliver photo before text without mistaking another worker's lease for ack."""
    photo = await _deliver_report_stage_once(report_id, "photo", photo_sender)
    if not photo:
        photo_state = get_report_delivery_stage_state(report_id, "photo")
        if photo_state is None:
            raise LegacyDeliveryStateInvalid(
                "report disappeared while checking photo delivery state"
            )
        if photo_state.get("delivered") is not True:
            # A different worker may currently own the photo lease, or this
            # stage may have been durably deferred. Do not overtake either case.
            return False, False

    text = await _deliver_report_stage_once(report_id, "text", text_sender)
    return photo, text
