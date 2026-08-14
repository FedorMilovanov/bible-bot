"""Report-only durable outbox drain for Telegram production wiring.

The combined legacy delivery drain also processes battle outbox entries. Reports
are migrated to the strict controller before PvP, so production needs a scoped
report worker that cannot accidentally deliver battles through the old protocol.
Telegram RetryAfter is translated into a durable future lease instead of an
immediate release. Durable stage evidence also repairs a missing aggregate
`admin_delivered` acknowledgement after a partial Mongo failure.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from legacy_delivery_worker import deliver_report_once
from legacy_report_delivery_repair import repair_report_delivery_aggregate
from report_integrity import ReportStoreUnavailable, get_pending_reports
from telegram_delivery_retry import send_with_durable_retry_after


class LegacyReportDeliveryQueueInvalid(RuntimeError):
    """Durable pending-report evidence is structurally invalid."""


@dataclass(frozen=True)
class ReportDeliveryDrainSummary:
    reports_seen: int = 0
    stage_sends: int = 0
    deferred: int = 0
    errors: tuple[str, ...] = ()


def _required_report_id(report: dict) -> str:
    report_id = report.get("_id") or report.get("report_id")
    if not isinstance(report_id, str) or not report_id:
        raise LegacyReportDeliveryQueueInvalid("pending report id is invalid")
    return report_id


def _error_text(identifier: str, exc: Exception) -> str:
    return f"report:{identifier}:{type(exc).__name__}:{exc}"[:500]


async def drain_pending_reports(
    *,
    photo_sender: Callable[[dict], Awaitable[Any]],
    text_sender: Callable[[dict], Awaitable[Any]],
    limit: int = 50,
) -> ReportDeliveryDrainSummary:
    """Drain only report outbox entries, isolating per-report delivery failures."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")

    errors: list[str] = []
    try:
        reports = await asyncio.to_thread(get_pending_reports, limit)
    except ReportStoreUnavailable as exc:
        return ReportDeliveryDrainSummary(
            errors=(f"report-list:<queue>:{type(exc).__name__}:{exc}"[:500],)
        )
    if not isinstance(reports, list):
        raise LegacyReportDeliveryQueueInvalid(
            "pending report listing returned invalid data"
        )

    async def durable_photo_sender(report: dict):
        return await send_with_durable_retry_after(photo_sender, report)

    async def durable_text_sender(report: dict):
        return await send_with_durable_retry_after(text_sender, report)

    stage_sends = 0
    deferred = 0
    for report in reports:
        if not isinstance(report, dict):
            errors.append(
                "report:<unknown>:LegacyReportDeliveryQueueInvalid:pending report is invalid"
            )
            continue
        identifier = str(report.get("_id") or report.get("report_id") or "<unknown>")
        try:
            report_id = _required_report_id(report)
            identifier = report_id
            # Repair the crash window where both stage obligations were already
            # settled but the aggregate admin_delivered write did not land.
            if await asyncio.to_thread(repair_report_delivery_aggregate, report_id):
                continue
            try:
                photo_sent, text_sent = await deliver_report_once(
                    report_id,
                    durable_photo_sender,
                    durable_text_sender,
                )
            except Exception:
                # A stage acknowledgement may have landed before its aggregate
                # write failed. Prove terminal stage evidence before surfacing
                # the error; if repair succeeds there is nothing left to send.
                if await asyncio.to_thread(repair_report_delivery_aggregate, report_id):
                    continue
                raise

            stage_sends += int(photo_sent) + int(text_sent)
            terminal = await asyncio.to_thread(
                repair_report_delivery_aggregate,
                report_id,
            )
            if not text_sent and not terminal:
                deferred += 1
        except Exception as exc:
            errors.append(_error_text(identifier, exc))

    return ReportDeliveryDrainSummary(
        reports_seen=len(reports),
        stage_sends=stage_sends,
        deferred=deferred,
        errors=tuple(errors),
    )
