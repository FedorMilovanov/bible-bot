"""Crash-safe Telegram broadcast acceptance and per-recipient delivery state.

A broadcast command is identified by the immutable Telegram update id. The
recipient snapshot is persisted before delivery, then materialized into separate
leased delivery rows. This limits a crash/retry to at-least-once uncertainty for
the one recipient whose Telegram send may have succeeded before its Mongo ack;
it cannot restart the whole broadcast from recipient zero.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

from pymongo import ASCENDING, ReturnDocument, UpdateOne
from pymongo.errors import DuplicateKeyError, PyMongoError

logger = logging.getLogger(__name__)

_RETENTION_SECONDS = 90 * 24 * 60 * 60
_FANOUT_CHUNK = 500
_MAX_TEXT_LENGTH = 3500
_MAX_RECIPIENT_SNAPSHOT_BYTES = 4 * 1024 * 1024


class BroadcastStoreUnavailable(RuntimeError):
    """Raised when durable broadcast acceptance/delivery cannot be proven."""


def _database():
    import database

    return database


def _collections():
    database = _database()
    db = getattr(database, "db", None)
    if db is None:
        raise BroadcastStoreUnavailable("broadcast storage is unavailable")
    return database, db["broadcasts"], db["broadcast_deliveries"]


def broadcast_id_for_update(update_id: int) -> str:
    if isinstance(update_id, bool) or not isinstance(update_id, int) or update_id < 0:
        raise ValueError("Telegram update id must be a non-negative integer")
    return f"telegram_update_{update_id}"


def _required_string(value, field: str, *, max_length: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    value = value.strip()
    if not value:
        raise ValueError(f"{field} is required")
    if len(value) > max_length:
        raise ValueError(f"{field} is too long")
    return value


def _recipient_snapshot(values) -> list[str]:
    if not isinstance(values, (list, tuple, set)):
        raise ValueError("recipient ids must be a sequence")
    recipients: set[str] = set()
    for raw in values:
        if isinstance(raw, bool):
            raise ValueError("recipient id is invalid")
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("recipient id is invalid") from exc
        if value <= 0:
            raise ValueError("recipient id is invalid")
        recipients.add(str(value))
    ordered = sorted(recipients, key=int)
    estimated_bytes = sum(len(item.encode("utf-8")) + 4 for item in ordered)
    if estimated_bytes > _MAX_RECIPIENT_SNAPSHOT_BYTES:
        raise ValueError("recipient snapshot is too large")
    return ordered


def _immutable_snapshot(doc: dict) -> dict:
    return {
        "admin_id": doc.get("admin_id"),
        "admin_chat_id": doc.get("admin_chat_id"),
        "text": doc.get("text"),
        "recipient_ids": doc.get("recipient_ids"),
    }


def _delivery_id(broadcast_id: str, user_id: str) -> str:
    return f"{broadcast_id}:{user_id}"


def ensure_broadcast_indexes() -> None:
    """Create only non-destructive indexes needed by the durable broadcaster."""
    _database_obj, broadcasts, deliveries = _collections()
    try:
        broadcasts.create_index(
            [("completed", ASCENDING), ("created_at_dt", ASCENDING)],
            name="idx_broadcast_pending",
        )
        deliveries.create_index(
            [("done", ASCENDING), ("lease_until", ASCENDING), ("created_at_dt", ASCENDING)],
            name="idx_broadcast_delivery_claim",
        )
        deliveries.create_index(
            [("broadcast_id", ASCENDING), ("done", ASCENDING)],
            name="idx_broadcast_delivery_parent",
        )
        broadcasts.create_index(
            [("retention_at_dt", ASCENDING)],
            name="ttl_broadcast_retention",
            expireAfterSeconds=_RETENTION_SECONDS,
        )
        deliveries.create_index(
            [("retention_at_dt", ASCENDING)],
            name="ttl_broadcast_delivery_retention",
            expireAfterSeconds=_RETENTION_SECONDS,
        )
    except PyMongoError as exc:
        logger.exception("broadcast index bootstrap failed")
        raise BroadcastStoreUnavailable("broadcast indexes are unavailable") from exc


def ensure_broadcast_fanout(broadcast: dict) -> dict:
    """Idempotently materialize all immutable recipient delivery rows."""
    if not isinstance(broadcast, dict):
        raise ValueError("broadcast must be a dict")
    broadcast_id = _required_string(
        broadcast.get("_id") or broadcast.get("broadcast_id"),
        "broadcast_id",
        max_length=128,
    )
    recipients = _recipient_snapshot(broadcast.get("recipient_ids"))
    database, broadcasts, deliveries = _collections()
    created_at = broadcast.get("created_at_dt")
    if not isinstance(created_at, datetime):
        raise BroadcastStoreUnavailable("broadcast creation time is invalid")

    try:
        for start in range(0, len(recipients), _FANOUT_CHUNK):
            chunk = recipients[start : start + _FANOUT_CHUNK]
            operations = [
                UpdateOne(
                    {"_id": _delivery_id(broadcast_id, user_id)},
                    {
                        "$setOnInsert": {
                            "_id": _delivery_id(broadcast_id, user_id),
                            "broadcast_id": broadcast_id,
                            "user_id": user_id,
                            "created_at_dt": created_at,
                            "done": False,
                            "delivered": False,
                            "attempts": 0,
                        }
                    },
                    upsert=True,
                )
                for user_id in chunk
            ]
            if operations:
                deliveries.bulk_write(operations, ordered=False)

        now = database._now_utc()
        result = broadcasts.update_one(
            {"_id": broadcast_id, "fanout_ready": {"$ne": True}},
            {"$set": {"fanout_ready": True, "fanout_ready_at": now}},
        )
        if result.modified_count != 1:
            current = broadcasts.find_one({"_id": broadcast_id}, {"fanout_ready": 1})
            if not isinstance(current, dict) or current.get("fanout_ready") is not True:
                raise BroadcastStoreUnavailable("broadcast fanout could not be proven complete")
        stored = broadcasts.find_one({"_id": broadcast_id})
        if not isinstance(stored, dict):
            raise BroadcastStoreUnavailable("broadcast disappeared after fanout")
        return stored
    except BroadcastStoreUnavailable:
        raise
    except PyMongoError as exc:
        logger.exception("broadcast fanout failed for %s", broadcast_id)
        raise BroadcastStoreUnavailable("broadcast fanout failed") from exc


def accept_broadcast_once(
    *,
    broadcast_id: str,
    admin_id: int,
    admin_chat_id: int,
    text: str,
    recipient_ids,
) -> tuple[dict, bool]:
    """Persist one immutable broadcast command and recipient snapshot exactly once."""
    broadcast_id = _required_string(broadcast_id, "broadcast_id", max_length=128)
    if isinstance(admin_id, bool) or not isinstance(admin_id, int) or admin_id <= 0:
        raise ValueError("admin_id is invalid")
    if isinstance(admin_chat_id, bool) or not isinstance(admin_chat_id, int):
        raise ValueError("admin_chat_id is invalid")
    text = _required_string(text, "text", max_length=_MAX_TEXT_LENGTH)
    recipients = _recipient_snapshot(recipient_ids)
    database, broadcasts, _deliveries = _collections()
    now = database._now_utc()
    doc = {
        "_id": broadcast_id,
        "broadcast_id": broadcast_id,
        "admin_id": str(admin_id),
        "admin_chat_id": str(admin_chat_id),
        "text": text,
        "recipient_ids": recipients,
        "recipient_count": len(recipients),
        "created_at_dt": now,
        "created_at": now.isoformat(),
        "fanout_ready": False,
        "completed": False,
    }

    created = False
    try:
        try:
            broadcasts.insert_one(doc)
            stored = doc
            created = True
        except DuplicateKeyError:
            stored = broadcasts.find_one({"_id": broadcast_id})
            if not isinstance(stored, dict):
                raise BroadcastStoreUnavailable("existing broadcast cannot be loaded") from None
            if _immutable_snapshot(stored) != _immutable_snapshot(doc):
                raise BroadcastStoreUnavailable(
                    "broadcast id is bound to different immutable content"
                ) from None
        return ensure_broadcast_fanout(stored), created
    except BroadcastStoreUnavailable:
        raise
    except PyMongoError as exc:
        logger.exception("broadcast acceptance failed for %s", broadcast_id)
        raise BroadcastStoreUnavailable("broadcast acceptance failed") from exc


def get_pending_broadcasts(limit: int = 20) -> list[dict]:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    _database_obj, broadcasts, _deliveries = _collections()
    try:
        return list(
            broadcasts.find({"completed": {"$ne": True}})
            .sort("created_at_dt", ASCENDING)
            .limit(limit)
        )
    except PyMongoError as exc:
        raise BroadcastStoreUnavailable("pending broadcast listing failed") from exc


def get_broadcast(broadcast_id: str) -> dict | None:
    broadcast_id = _required_string(broadcast_id, "broadcast_id", max_length=128)
    _database_obj, broadcasts, _deliveries = _collections()
    try:
        value = broadcasts.find_one({"_id": broadcast_id})
        return value if isinstance(value, dict) else None
    except PyMongoError as exc:
        raise BroadcastStoreUnavailable("broadcast lookup failed") from exc


def claim_next_broadcast_delivery(
    *,
    broadcast_id: str | None = None,
    lease_seconds: int = 120,
) -> dict | None:
    if broadcast_id is not None:
        broadcast_id = _required_string(broadcast_id, "broadcast_id", max_length=128)
    if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or lease_seconds <= 0:
        raise ValueError("lease_seconds must be a positive integer")
    database, _broadcasts, deliveries = _collections()
    now = database._now_utc()
    token = uuid.uuid4().hex
    query = {
        "done": {"$ne": True},
        "$or": [
            {"lease_until": {"$exists": False}},
            {"lease_until": {"$lte": now}},
        ],
    }
    if broadcast_id is not None:
        query["broadcast_id"] = broadcast_id
    try:
        claimed = deliveries.find_one_and_update(
            query,
            {
                "$set": {
                    "claim_token": token,
                    "lease_until": now + timedelta(seconds=lease_seconds),
                    "last_attempt_at": now,
                },
                "$inc": {"attempts": 1},
            },
            sort=[("created_at_dt", ASCENDING), ("_id", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )
        return claimed if isinstance(claimed, dict) else None
    except PyMongoError as exc:
        raise BroadcastStoreUnavailable("broadcast delivery claim failed") from exc


def _settle_delivery(
    delivery_id: str,
    claim_token: str,
    *,
    delivered: bool,
    error: str = "",
) -> bool:
    delivery_id = _required_string(delivery_id, "delivery_id", max_length=256)
    claim_token = _required_string(claim_token, "claim_token", max_length=128)
    database, _broadcasts, deliveries = _collections()
    now = database._now_utc()
    update = {
        "$set": {
            "done": True,
            "delivered": bool(delivered),
            "done_at_dt": now,
        },
        "$unset": {"claim_token": "", "lease_until": "", "last_error": ""},
    }
    if error:
        update["$set"]["terminal_error"] = str(error)[:500]
    try:
        result = deliveries.update_one(
            {"_id": delivery_id, "done": {"$ne": True}, "claim_token": claim_token},
            update,
        )
        if result.modified_count == 1:
            return True
        current = deliveries.find_one({"_id": delivery_id}, {"done": 1})
        return isinstance(current, dict) and current.get("done") is True
    except PyMongoError as exc:
        raise BroadcastStoreUnavailable("broadcast delivery acknowledgement failed") from exc


def mark_broadcast_delivery_delivered(delivery_id: str, claim_token: str) -> bool:
    return _settle_delivery(delivery_id, claim_token, delivered=True)


def mark_broadcast_delivery_terminal_failure(
    delivery_id: str,
    claim_token: str,
    *,
    error: str,
) -> bool:
    return _settle_delivery(delivery_id, claim_token, delivered=False, error=error)


def release_broadcast_delivery(
    delivery_id: str,
    claim_token: str,
    *,
    error: str = "",
) -> bool:
    delivery_id = _required_string(delivery_id, "delivery_id", max_length=256)
    claim_token = _required_string(claim_token, "claim_token", max_length=128)
    _database_obj, _broadcasts, deliveries = _collections()
    try:
        result = deliveries.update_one(
            {"_id": delivery_id, "done": {"$ne": True}, "claim_token": claim_token},
            {
                "$set": {"last_error": str(error or "")[:500]},
                "$unset": {"claim_token": "", "lease_until": ""},
            },
        )
        return result.modified_count == 1
    except PyMongoError as exc:
        raise BroadcastStoreUnavailable("broadcast delivery release failed") from exc


def sync_broadcast_completion(broadcast_id: str) -> dict:
    """Mark a fanout complete only after every delivery row is terminal."""
    broadcast_id = _required_string(broadcast_id, "broadcast_id", max_length=128)
    database, broadcasts, deliveries = _collections()
    try:
        parent = broadcasts.find_one({"_id": broadcast_id})
        if not isinstance(parent, dict):
            raise BroadcastStoreUnavailable("broadcast is missing")
        if parent.get("fanout_ready") is not True:
            return {"completed": False, "delivered": 0, "failed": 0}
        remaining = deliveries.count_documents(
            {"broadcast_id": broadcast_id, "done": {"$ne": True}},
            limit=1,
        )
        delivered = deliveries.count_documents(
            {"broadcast_id": broadcast_id, "done": True, "delivered": True}
        )
        failed = deliveries.count_documents(
            {"broadcast_id": broadcast_id, "done": True, "delivered": {"$ne": True}}
        )
        if remaining:
            return {"completed": False, "delivered": delivered, "failed": failed}

        now = database._now_utc()
        broadcasts.update_one(
            {"_id": broadcast_id, "completed": {"$ne": True}},
            {
                "$set": {
                    "completed": True,
                    "completed_at_dt": now,
                    "retention_at_dt": now,
                    "delivered_count": delivered,
                    "failed_count": failed,
                }
            },
        )
        # Parent completion is written first. If this retention follow-up fails,
        # rows leak conservatively instead of expiring and being re-created/sent.
        deliveries.update_many(
            {"broadcast_id": broadcast_id},
            {"$set": {"retention_at_dt": now}},
        )
        return {"completed": True, "delivered": delivered, "failed": failed}
    except BroadcastStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise BroadcastStoreUnavailable("broadcast completion sync failed") from exc
