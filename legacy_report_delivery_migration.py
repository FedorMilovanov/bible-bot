"""Lazy compatibility bridge for historical pending report outbox records.

Older ``database.insert_report`` rows persisted the report text but had no
per-stage ``delivery`` object. Once the durable outbox worker is enabled those
rows must remain recoverable instead of becoming permanently undeliverable.
This module initializes only a completely missing delivery object, never repairs
or overwrites a partially present/malformed one.
"""
from __future__ import annotations

from pymongo.errors import PyMongoError

from report_integrity import claim_report_delivery_stage as _claim_stage


class LegacyReportDeliveryMigrationUnavailable(RuntimeError):
    """Historical report delivery state cannot currently be migrated/read."""


class LegacyReportDeliveryStateInvalid(RuntimeError):
    """Historical report delivery evidence is contradictory or malformed."""


def _collection():
    import database

    reports = getattr(database, "reports_collection", None)
    if reports is None:
        raise LegacyReportDeliveryMigrationUnavailable("report storage is unavailable")
    return reports


def _required_report_id(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("report_id is required")
    return value.strip()


def _durable_photo_id(value) -> str | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str) or not value.strip():
        raise LegacyReportDeliveryStateInvalid("legacy report photo_file_id is invalid")
    return value.strip()


def _validate_stage(state, stage: str) -> None:
    if not isinstance(state, dict):
        raise LegacyReportDeliveryStateInvalid(f"report {stage} delivery state is invalid")
    delivered = state.get("delivered")
    attempts = state.get("attempts", 0)
    if not isinstance(delivered, bool):
        raise LegacyReportDeliveryStateInvalid(
            f"report {stage} delivered flag is invalid"
        )
    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
        raise LegacyReportDeliveryStateInvalid(
            f"report {stage} attempts value is invalid"
        )


def _validated_delivery_document(doc: dict, report_id: str) -> dict:
    if not isinstance(doc, dict) or doc.get("_id") != report_id:
        raise LegacyReportDeliveryStateInvalid("report delivery document is invalid")
    if doc.get("admin_delivered") is True:
        return doc
    delivery = doc.get("delivery")
    if not isinstance(delivery, dict):
        raise LegacyReportDeliveryStateInvalid("report delivery state is invalid")
    photo = delivery.get("photo")
    text = delivery.get("text")
    _validate_stage(photo, "photo")
    _validate_stage(text, "text")
    photo_file_id = _durable_photo_id(doc.get("photo_file_id"))
    if photo.get("delivered") is False and photo_file_id is None:
        raise LegacyReportDeliveryStateInvalid(
            "pending report photo has no durable photo_file_id"
        )
    return doc


def ensure_report_delivery_state(report_id: str) -> dict | None:
    """Idempotently initialize a completely missing historical delivery object.

    Historical reports had no durable attachment id. Therefore a legacy row with
    no ``photo_file_id`` has no recoverable photo obligation: photo starts
    acknowledged and text starts pending. If a transitional row does carry a
    durable non-empty ``photo_file_id``, both stages start pending.

    A partially present or malformed delivery object is never guessed/repaired;
    it fails closed so evidence is not overwritten.
    """
    report_id = _required_report_id(report_id)
    reports = _collection()
    try:
        doc = reports.find_one({"_id": report_id})
        if doc is None:
            return None
        if doc.get("admin_delivered") is True:
            return doc
        if "delivery" not in doc:
            photo_file_id = _durable_photo_id(doc.get("photo_file_id"))
            delivery = {
                "photo": {
                    "delivered": photo_file_id is None,
                    "attempts": 0,
                },
                "text": {"delivered": False, "attempts": 0},
            }
            write = reports.update_one(
                {
                    "_id": report_id,
                    "admin_delivered": {"$ne": True},
                    "delivery": {"$exists": False},
                },
                {"$set": {"delivery": delivery}},
            )
            if write.modified_count not in {0, 1}:
                raise LegacyReportDeliveryMigrationUnavailable(
                    "legacy report delivery migration returned invalid write result"
                )
            doc = reports.find_one({"_id": report_id})
            if doc is None:
                return None
        return _validated_delivery_document(doc, report_id)
    except (LegacyReportDeliveryMigrationUnavailable, LegacyReportDeliveryStateInvalid):
        raise
    except PyMongoError as exc:
        raise LegacyReportDeliveryMigrationUnavailable(
            "legacy report delivery migration failed"
        ) from exc


def claim_report_delivery_stage_compatible(
    report_id: str,
    stage: str,
    *,
    lease_seconds: int = 120,
) -> dict | None:
    """Claim through the normal store after lazy legacy-state initialization."""
    doc = ensure_report_delivery_state(report_id)
    if doc is None or doc.get("admin_delivered") is True:
        return None
    return _claim_stage(report_id, stage, lease_seconds=lease_seconds)
