"""Async orchestration over durable battle/report delivery leases.

The module knows nothing about Telegram handlers or application lifecycle. A
caller injects the actual send coroutine. Storage decides ownership, lease and
acknowledgement. Ordinary sender failures release the lease for retry; an
explicit LegacyDeliveryDeferred signal preserves a future retry in Mongo; an
explicit LegacyDeliveryPermanentFailure settles the delivery obligation while
retaining a durable terminal-failure marker. If a remote send completed but the
Mongo acknowledgement later fails, the lease is left to expire rather than
being released immediately, reducing duplicate-send risk.

All durable storage boundaries are synchronous PyMongo operations. They are
executed in worker threads so lease acquisition/acknowledgement cannot stall the
PTB asyncio loop around latency-sensitive Telegram sends.
"""
from __future__ import annotations

import asyncio
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
from legacy_delivery_terminal import (
    settle_battle_result_delivery_failure,
    settle_report_delivery_stage_failure,
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
    """Remote send or terminal settlement could not be durably acknowledged."""


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


class LegacyDeliveryPermanentFailure(RuntimeError):
    """Sender proves the remote destination/payload is permanently undeliverable."""

    def __init__(self, detail: str):
        value = str(detail or "").strip()
        if not value:
            raise ValueError("permanent failure detail is required")
        self.detail = value[:500]
        super().__init__(self.detail)


async def deliver_battle_recipient_once(
    battle_id: str,
    user_id: int,
    sender: Callable[[dict, str], Awaitable[Any]],
) -> bool:
    """Attempt one leased battle-result delivery for one participant."""
    claim = await asyncio.to_thread(claim_battle_result_delivery, battle_id, user_id)
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
    except LegacyDeliveryPermanentFailure as exc:
        settled = await asyncio.to_thread(
            settle_battle_result_delivery_failure,
            battle_id,
            user_id,
            token,
            error=exc.detail,
        )
        if not settled:
            raise LegacyDeliveryAcknowledgementPending(
                "battle permanent failure could not be durably settled"
            ) from exc
        return False
    except LegacyDeliveryDeferred as exc:
        deferred = await asyncio.to_thread(
            defer_battle_result_delivery,
            battle_id,
            user_id,
            token,
            delay_seconds=exc.delay_seconds,
            error=exc.detail or str(exc),
        )
        if not deferred:
            raise LegacyDeliveryAcknowledgementPending(
                "battle result deferral could not be acknowledged"
            ) from exc
        return False
    except Exception as exc:
        await asyncio.to_thread(
            release_battle_result_delivery,
            battle_id,
            user_id,
            token,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise

    acknowledged = await asyncio.to_thread(
        mark_battle_result_delivered,
        battle_id,
        user_id,
        token,
    )
    if not acknowledged:
        raise LegacyDeliveryAcknowledgementPending(
            "battle result was sent but acknowledgement is pending"
        )
    return True


async def _deliver_report_stage_once(
    report_id: str,
    stage: str,
    sender: Callable[[dict], Awaitable[Any]],
) -> bool:
    claim = await asyncio.to_thread(claim_report_delivery_stage, report_id, stage)
    if claim is None:
        return False
    report = claim.get("report")
    token = claim.get("claim_token")
    if not isinstance(report, dict):
        raise LegacyDeliveryStateInvalid("report delivery claim is malformed")
    if not isinstance(token, str) or not token:
        raise LegacyDeliveryStateInvalid("report delivery claim token is missing")
    if stage == "photo" and not report.get("photo_file_id"):
        await asyncio.to_thread(
            release_report_delivery_stage,
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
    except LegacyDeliveryPermanentFailure as exc:
        settled = await asyncio.to_thread(
            settle_report_delivery_stage_failure,
            report_id,
            stage,
            token,
            error=exc.detail,
        )
        if not settled:
            raise LegacyDeliveryAcknowledgementPending(
                f"report {stage} permanent failure could not be durably settled"
            ) from exc
        return False
    except LegacyDeliveryDeferred as exc:
        deferred = await asyncio.to_thread(
            defer_report_delivery_stage,
            report_id,
            stage,
            token,
            delay_seconds=exc.delay_seconds,
            error=exc.detail or str(exc),
        )
        if not deferred:
            raise LegacyDeliveryAcknowledgementPending(
                f"report {stage} deferral could not be acknowledged"
            ) from exc
        return False
    except Exception as exc:
        await asyncio.to_thread(
            release_report_delivery_stage,
            report_id,
            stage,
            token,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise

    acknowledged = await asyncio.to_thread(
        mark_report_delivery_stage_delivered,
        report_id,
        stage,
        token,
    )
    if not acknowledged:
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
        photo_state = await asyncio.to_thread(
            get_report_delivery_stage_state,
            report_id,
            "photo",
        )
        if photo_state is None:
            raise LegacyDeliveryStateInvalid(
                "report disappeared while checking photo delivery state"
            )
        if photo_state.get("delivered") is not True:
            # A different worker may own the photo lease, or this stage may be
            # durably deferred. A permanent failure is settled as delivered=True
            # plus terminal_failed=True so text can still be attempted.
            return False, False

    text = await _deliver_report_stage_once(report_id, "text", text_sender)
    return photo, text
