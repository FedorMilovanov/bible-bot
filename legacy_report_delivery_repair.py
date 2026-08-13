"""Recover the report aggregate delivery flag from durable stage evidence."""
from __future__ import annotations

from pymongo.errors import PyMongoError

from report_integrity import ReportStoreUnavailable


def repair_report_delivery_aggregate(report_id: str) -> bool:
    """Return True when both stage obligations are terminal and aggregate is proven."""
    if not isinstance(report_id, str) or not report_id.strip():
        raise ValueError("report_id is required")

    import database

    reports = getattr(database, "reports_collection", None)
    if reports is None:
        raise ReportStoreUnavailable("report storage is unavailable")
    try:
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
            raise ReportStoreUnavailable("report disappeared during aggregate repair")
        if current.get("admin_delivered") is True:
            return True
        delivery = current.get("delivery")
        photo = delivery.get("photo") if isinstance(delivery, dict) else None
        text = delivery.get("text") if isinstance(delivery, dict) else None
        if not isinstance(photo, dict) or not isinstance(text, dict):
            raise ReportStoreUnavailable("report delivery state is malformed")
        if photo.get("delivered") is not True or text.get("delivered") is not True:
            return False

        now = database._now_utc()
        fields = {
            "admin_delivered": True,
            "admin_delivered_at": now,
        }
        if photo.get("terminal_failed") is True or text.get("terminal_failed") is True:
            fields["admin_delivery_failed"] = True
        result = reports.update_one(
            {"_id": report_id, "admin_delivered": {"$ne": True}},
            {"$set": fields},
        )
        if result.modified_count == 1:
            return True
        verified = reports.find_one({"_id": report_id}, {"admin_delivered": 1})
        return isinstance(verified, dict) and verified.get("admin_delivered") is True
    except ReportStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise ReportStoreUnavailable("report delivery aggregate repair failed") from exc
