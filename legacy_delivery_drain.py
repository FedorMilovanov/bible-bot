"""Explicit drain orchestration for retained report and battle outboxes.

Nothing starts a background task on import. The application lifecycle must call
``drain_pending_deliveries`` explicitly and inject the real Telegram senders.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from battle_integrity import BattleStoreUnavailable, get_pending_final_battles
from legacy_delivery_worker import deliver_battle_recipient_once, deliver_report_once
from report_integrity import ReportStoreUnavailable, get_pending_reports


class LegacyDeliveryQueueInvalid(RuntimeError):
    """Durable pending-delivery evidence is structurally invalid."""


@dataclass(frozen=True)
class DeliveryDrainSummary:
    reports_seen: int = 0
    report_stage_sends: int = 0
    battles_seen: int = 0
    battle_recipient_sends: int = 0
    deferred: int = 0
    errors: tuple[str, ...] = ()


def _required_id(value, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise LegacyDeliveryQueueInvalid(f"pending {field} id is invalid")
    return value


def _participant_id(value, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LegacyDeliveryQueueInvalid(f"pending battle {field} is invalid")
    return value


def _error_text(kind: str, identifier: str, exc: Exception) -> str:
    return f"{kind}:{identifier}:{type(exc).__name__}:{exc}"[:500]


def _list_reports(limit: int, errors: list[str]) -> list[dict]:
    """Isolate only a known report-store outage from the battle queue."""
    try:
        reports = get_pending_reports(limit)
    except ReportStoreUnavailable as exc:
        errors.append(_error_text("report-list", "<queue>", exc))
        return []
    if not isinstance(reports, list):
        raise LegacyDeliveryQueueInvalid("pending report listing returned invalid data")
    return reports


def _list_battles(limit: int, errors: list[str]) -> list[dict]:
    """Isolate only a known battle-store outage from the report queue."""
    try:
        battles = get_pending_final_battles(limit)
    except BattleStoreUnavailable as exc:
        errors.append(_error_text("battle-list", "<queue>", exc))
        return []
    if not isinstance(battles, list):
        raise LegacyDeliveryQueueInvalid("pending battle listing returned invalid data")
    return battles


async def drain_pending_deliveries(
    *,
    battle_sender: Callable[[dict, str], Awaitable[Any]],
    report_photo_sender: Callable[[dict], Awaitable[Any]],
    report_text_sender: Callable[[dict], Awaitable[Any]],
    limit: int = 50,
) -> DeliveryDrainSummary:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")

    errors: list[str] = []
    reports = _list_reports(limit, errors)
    battles = _list_battles(limit, errors)
    report_stage_sends = 0
    battle_recipient_sends = 0
    deferred = 0

    for report in reports:
        if not isinstance(report, dict):
            errors.append("report:<unknown>:LegacyDeliveryQueueInvalid:pending report is invalid")
            continue
        try:
            report_id = _required_id(report.get("_id") or report.get("report_id"), "report")
            photo_sent, text_sent = await deliver_report_once(
                report_id,
                report_photo_sender,
                report_text_sender,
            )
            report_stage_sends += int(photo_sent) + int(text_sent)
            if not photo_sent and not text_sent:
                deferred += 1
        except Exception as exc:
            identifier = str(report.get("_id") or report.get("report_id") or "<unknown>")
            errors.append(_error_text("report", identifier, exc))

    for battle in battles:
        if not isinstance(battle, dict):
            errors.append("battle:<unknown>:LegacyDeliveryQueueInvalid:pending battle is invalid")
            continue
        battle_id = str(battle.get("_id") or "<unknown>")
        try:
            battle_id = _required_id(battle.get("_id"), "battle")
            participants = (
                _participant_id(battle.get("creator_id"), "creator_id"),
                _participant_id(battle.get("opponent_id"), "opponent_id"),
            )
            if participants[0] == participants[1]:
                raise LegacyDeliveryQueueInvalid("pending battle participants are identical")
        except Exception as exc:
            errors.append(_error_text("battle", battle_id, exc))
            continue
        for participant_id in participants:
            try:
                sent = await deliver_battle_recipient_once(
                    battle_id,
                    participant_id,
                    battle_sender,
                )
                battle_recipient_sends += int(sent)
                if not sent:
                    deferred += 1
            except Exception as exc:
                errors.append(_error_text("battle", f"{battle_id}/{participant_id}", exc))

    return DeliveryDrainSummary(
        reports_seen=len(reports),
        report_stage_sends=report_stage_sends,
        battles_seen=len(battles),
        battle_recipient_sends=battle_recipient_sends,
        deferred=deferred,
        errors=tuple(errors),
    )
