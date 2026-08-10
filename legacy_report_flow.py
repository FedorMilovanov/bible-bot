"""Crash-safe report submission + immediate delivery attempt orchestration."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from legacy_delivery_worker import deliver_report_once
from legacy_report_submit import accept_report_draft_once
from report_integrity import get_report_delivery_stage_state


@dataclass(frozen=True)
class ReportSubmissionOutcome:
    report_id: str
    accepted: bool
    delivered: bool
    delivery_pending: bool
    delivery_error: str | None = None


def _required_report_id(stored: dict) -> str:
    if not isinstance(stored, dict):
        raise RuntimeError("durable report acceptance returned invalid state")
    report_id = stored.get("_id") or stored.get("report_id")
    if not isinstance(report_id, str) or not report_id:
        raise RuntimeError("durable report acceptance returned no report id")
    return report_id


def _delivery_complete(report_id: str) -> bool:
    photo = get_report_delivery_stage_state(report_id, "photo")
    text = get_report_delivery_stage_state(report_id, "text")
    return (
        isinstance(photo, dict)
        and isinstance(text, dict)
        and photo.get("delivered") is True
        and text.get("delivered") is True
    )


async def submit_report_once(
    *,
    user_id: int,
    username: str | None,
    first_name: str | None,
    draft: dict,
    context: dict | None,
    photo_sender: Callable[[dict], Awaitable[Any]],
    text_sender: Callable[[dict], Awaitable[Any]],
) -> ReportSubmissionOutcome:
    stored = accept_report_draft_once(
        user_id=user_id,
        username=username,
        first_name=first_name,
        draft=draft,
        context=context,
    )
    report_id = _required_report_id(stored)
    delivery_error = None
    try:
        await deliver_report_once(report_id, photo_sender, text_sender)
    except Exception as exc:
        delivery_error = f"{type(exc).__name__}: {exc}"[:500]
    try:
        delivered = _delivery_complete(report_id)
    except Exception as exc:
        delivered = False
        if delivery_error is None:
            delivery_error = f"{type(exc).__name__}: {exc}"[:500]
    return ReportSubmissionOutcome(
        report_id=report_id,
        accepted=True,
        delivered=delivered,
        delivery_pending=not delivered,
        delivery_error=delivery_error,
    )
