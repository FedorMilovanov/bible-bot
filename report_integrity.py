"""Crash-safe acceptance and admin-delivery primitives for user reports.

The legacy handler kept the optional Telegram photo file id only in RAM and
removed the draft before Mongo acceptance. This module gives a report a stable
idempotency key, persists all delivery inputs first, and tracks photo/text
admin delivery independently. Telegram sends are inherently at-least-once
because sendMessage/sendPhoto expose no idempotency key, but durable per-stage
leases prevent concurrent duplicate workers and preserve retry evidence.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

logger = logging.getLogger(__name__)

_REPORT_TYPES = frozenset({"bug", "idea", "question"})
_DELIVERY_STAGES = frozenset({"photo", "text"})


class ReportStoreUnavailable(RuntimeError):
    """Raised when durable report acceptance or delivery state is unavailable."""


def _database():
    import database

    return database


def _collections():
    database = _database()
    reports = getattr(database, "reports_collection", None)
    users = getattr(database, "collection", None)
    if reports is None or users is None:
        raise ReportStoreUnavailable("report storage is unavailable")
    return database, reports, users


def new_report_id() -> str:
    """Create the stable id that must be memoized in the user's report draft."""
    return str(uuid.uuid4())


def _required_string(value, field: str, *, max_length: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    value = value.strip()
    if not value:
        raise ValueError(f"{field} is required")
    if len(value) > max_length:
        raise ValueError(f"{field} is too long")
    return value


def _optional_string(value, field: str, *, max_length: int) -> str | None:
    if value is None or value == "":
        return None
    return _required_string(value, field, max_length=max_length)


def _immutable_snapshot(doc: dict) -> dict:
    return {
        "user_id": doc.get("user_id"),
        "type": doc.get("type"),
        "text": doc.get("text"),
        "photo_file_id": doc.get("photo_file_id"),
    }


def accept_report_once(
    *,
    report_id: str,
    user_id: int,
    username: str | None,
    first_name: str | None,
    report_type: str,
    text: str,
    photo_file_id: str | None = None,
    context: dict | None = None,
    update_cooldown: bool = True,
) -> dict:
    """Persist one report and its attachment exactly once by stable report id.

    The report document is always the durable source of truth and is written
    first. Normal report flows keep ``update_cooldown=True`` so the user-level
    cooldown is a recoverable monotonic follow-up (`$max`). Other durable report
    ingress paths may opt out when they historically did not consume that
    cooldown; their acceptance then finishes after the report insert/replay.
    """
    report_id = _required_string(report_id, "report_id", max_length=128)
    if report_type not in _REPORT_TYPES:
        raise ValueError("unsupported report type")
    text = _required_string(text, "text", max_length=2000)
    photo_file_id = _optional_string(photo_file_id, "photo_file_id", max_length=1024)
    if context is None:
        context = {}
    if not isinstance(context, dict):
        raise ValueError("context must be a dict")
    if not isinstance(update_cooldown, bool):
        raise ValueError("update_cooldown must be a boolean")

    database, reports, users = _collections()
    now = database._now_utc()
    uid = database._uid(user_id)
    doc = {
        "_id": report_id,
        "report_id": report_id,
        "user_id": uid,
        "username": username or "",
        "first_name": first_name or "",
        "type": report_type,
        "text": text,
        "photo_file_id": photo_file_id,
        "context": context,
        "created_at": now.isoformat(),
        "created_at_dt": now,
        "admin_delivered": False,
        "delivery": {
            "photo": {
                # A report without a photo has no photo delivery obligation.
                "delivered": photo_file_id is None,
                "attempts": 0,
            },
            "text": {"delivered": False, "attempts": 0},
        },
    }

    try:
        try:
            reports.insert_one(doc)
            stored = doc
        except DuplicateKeyError:
            stored = reports.find_one({"_id": report_id})
            if not isinstance(stored, dict):
                raise ReportStoreUnavailable(
                    "existing report receipt cannot be loaded"
                ) from None
            if _immutable_snapshot(stored) != _immutable_snapshot(doc):
                raise ReportStoreUnavailable(
                    "report id is bound to different immutable content"
                ) from None

        created_at = stored.get("created_at_dt")
        if not isinstance(created_at, datetime):
            raise ReportStoreUnavailable("durable report creation time is invalid")
        if not update_cooldown:
            return stored

        cooldown = users.update_one(
            {"_id": uid},
            {"$max": {"last_report_at": created_at}},
        )
        if cooldown.modified_count != 1:
            user = users.find_one({"_id": uid}, {"last_report_at": 1})
            durable_last = user.get("last_report_at") if isinstance(user, dict) else None
            if not isinstance(durable_last, datetime) or durable_last < created_at:
                raise ReportStoreUnavailable("report persisted but cooldown is not durable")
        return stored
    except ReportStoreUnavailable:
        raise
    except PyMongoError as exc:
        logger.exception("durable report acceptance failed for %s", report_id)
        raise ReportStoreUnavailable("report acceptance failed") from exc


def claim_report_delivery_stage(
    report_id: str,
    stage: str,
    *,
    lease_seconds: int = 120,
) -> dict | None:
    """Lease one pending photo/text delivery stage for the administrator."""
    report_id = _required_string(report_id, "report_id", max_length=128)
    if stage not in _DELIVERY_STAGES:
        raise ValueError("unsupported report delivery stage")
    if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or lease_seconds <= 0:
        raise ValueError("lease_seconds must be a positive integer")

    database, reports, _users = _collections()
    now = database._now_utc()
    token = uuid.uuid4().hex
    lease_until = now + timedelta(seconds=lease_seconds)
    path = f"delivery.{stage}"
    try:
        claimed = reports.find_one_and_update(
            {
                "_id": report_id,
                f"{path}.delivered": {"$ne": True},
                "$or": [
                    {f"{path}.lease_until": {"$exists": False}},
                    {f"{path}.lease_until": {"$lte": now}},
                ],
            },
            {
                "$set": {
                    f"{path}.claim_token": token,
                    f"{path}.lease_until": lease_until,
                    f"{path}.last_attempt_at": now,
                },
                "$inc": {f"{path}.attempts": 1},
            },
            return_document=ReturnDocument.AFTER,
        )
        if claimed is None:
            return None
        return {"report": claimed, "stage": stage, "claim_token": token}
    except PyMongoError as exc:
        logger.exception("failed to lease report %s stage %s", report_id, stage)
        raise ReportStoreUnavailable("report delivery claim failed") from exc


def get_report_delivery_stage_state(report_id: str, stage: str) -> dict | None:
    """Read one durable delivery stage without taking or changing its lease."""
    report_id = _required_string(report_id, "report_id", max_length=128)
    if stage not in _DELIVERY_STAGES:
        raise ValueError("unsupported report delivery stage")
    _database_obj, reports, _users = _collections()
    try:
        report = reports.find_one(
            {"_id": report_id},
            {f"delivery.{stage}": 1, "photo_file_id": 1, "report_id": 1},
        )
        if report is None:
            return None
        delivery = report.get("delivery")
        state = delivery.get(stage) if isinstance(delivery, dict) else None
        if not isinstance(state, dict):
            raise ReportStoreUnavailable("report delivery stage state is missing")
        delivered = state.get("delivered")
        attempts = state.get("attempts", 0)
        if not isinstance(delivered, bool):
            raise ReportStoreUnavailable("report delivery stage delivered flag is invalid")
        if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
            raise ReportStoreUnavailable("report delivery stage attempts value is invalid")
        return {
            "report_id": report.get("report_id") or report.get("_id"),
            "stage": stage,
            "delivered": delivered,
            "claim_token": state.get("claim_token"),
            "lease_until": state.get("lease_until"),
            "attempts": attempts,
            "photo_file_id": report.get("photo_file_id"),
        }
    except ReportStoreUnavailable:
        raise
    except PyMongoError as exc:
        logger.exception("failed to read report %s stage %s", report_id, stage)
        raise ReportStoreUnavailable("report delivery state lookup failed") from exc


def _sync_admin_delivered(reports, report_id: str, now: datetime) -> None:
    """Promote the legacy aggregate flag only after both durable stages are done."""
    current = reports.find_one(
        {"_id": report_id},
        {"delivery.photo.delivered": 1, "delivery.text.delivered": 1, "admin_delivered": 1},
    )
    if not isinstance(current, dict):
        raise ReportStoreUnavailable("report disappeared during delivery acknowledgement")
    delivery = current.get("delivery")
    if not isinstance(delivery, dict):
        raise ReportStoreUnavailable("report delivery state is missing")
    photo = delivery.get("photo")
    text = delivery.get("text")
    if not isinstance(photo, dict) or not isinstance(text, dict):
        raise ReportStoreUnavailable("report delivery state is malformed")
    if photo.get("delivered") is True and text.get("delivered") is True:
        reports.update_one(
            {"_id": report_id, "admin_delivered": {"$ne": True}},
            {"$set": {"admin_delivered": True, "admin_delivered_at": now}},
        )


def mark_report_delivery_stage_delivered(
    report_id: str,
    stage: str,
    claim_token: str,
) -> bool:
    """Acknowledge a leased stage after its Telegram send succeeds."""
    report_id = _required_string(report_id, "report_id", max_length=128)
    if stage not in _DELIVERY_STAGES:
        raise ValueError("unsupported report delivery stage")
    claim_token = _required_string(claim_token, "claim_token", max_length=128)
    database, reports, _users = _collections()
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
                    f"{path}.delivered_at": now,
                },
                "$unset": {
                    f"{path}.claim_token": "",
                    f"{path}.lease_until": "",
                    f"{path}.last_error": "",
                },
            },
        )
        if result.modified_count != 1:
            existing = reports.find_one({"_id": report_id}, {f"{path}.delivered": 1})
            delivery = existing.get("delivery", {}) if isinstance(existing, dict) else {}
            stage_state = delivery.get(stage, {}) if isinstance(delivery, dict) else {}
            if not isinstance(stage_state, dict) or stage_state.get("delivered") is not True:
                return False
        _sync_admin_delivered(reports, report_id, now)
        return True
    except ReportStoreUnavailable:
        raise
    except PyMongoError as exc:
        logger.exception("failed to acknowledge report %s stage %s", report_id, stage)
        raise ReportStoreUnavailable("report delivery acknowledgement failed") from exc


def release_report_delivery_stage(
    report_id: str,
    stage: str,
    claim_token: str,
    *,
    error: str = "",
) -> bool:
    """Release a failed stage lease while retaining bounded diagnostic text."""
    report_id = _required_string(report_id, "report_id", max_length=128)
    if stage not in _DELIVERY_STAGES:
        raise ValueError("unsupported report delivery stage")
    claim_token = _required_string(claim_token, "claim_token", max_length=128)
    _database_obj, reports, _users = _collections()
    path = f"delivery.{stage}"
    try:
        result = reports.update_one(
            {
                "_id": report_id,
                f"{path}.delivered": {"$ne": True},
                f"{path}.claim_token": claim_token,
            },
            {
                "$set": {f"{path}.last_error": str(error or "")[:500]},
                "$unset": {
                    f"{path}.claim_token": "",
                    f"{path}.lease_until": "",
                },
            },
        )
        return result.modified_count == 1
    except PyMongoError as exc:
        logger.exception("failed to release report %s stage %s", report_id, stage)
        raise ReportStoreUnavailable("report delivery release failed") from exc


def get_pending_reports(limit: int = 50) -> list[dict]:
    """Return durably accepted reports that still need admin delivery."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    _database_obj, reports, _users = _collections()
    try:
        return list(
            reports.find({"admin_delivered": {"$ne": True}})
            .sort("created_at_dt", 1)
            .limit(limit)
        )
    except PyMongoError as exc:
        logger.exception("failed to list pending reports")
        raise ReportStoreUnavailable("pending report listing failed") from exc
