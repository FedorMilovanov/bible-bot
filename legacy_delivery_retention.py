"""State-aware TTL migration for durable battle/report delivery evidence.

Legacy indexes expired battles/reports solely from creation time. Once delivery
became crash-recoverable, that contract became unsafe: an undelivered outbox
entry must never disappear just because it is old. These migrations preserve
pending evidence indefinitely and let Mongo TTL clean only already-delivered
terminal documents.
"""
from __future__ import annotations

import logging

from pymongo import ASCENDING
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

_BATTLE_LEGACY_TTL = "ttl_battles_created_at"
_BATTLE_DELIVERED_TTL = "ttl_battles_delivered_created_at"
_BATTLE_RETENTION_SECONDS = 30 * 24 * 60 * 60
_BATTLE_DELIVERED_FILTER = {
    "status": "finalized",
    "result_delivery.creator.delivered": True,
    "result_delivery.opponent.delivered": True,
}

_REPORT_LEGACY_TTL = "ttl_reports_created_at"
_REPORT_DELIVERED_TTL = "ttl_reports_delivered_created_at"
_REPORT_RETENTION_SECONDS = 90 * 24 * 60 * 60
_REPORT_DELIVERED_FILTER = {"admin_delivered": True}


class DeliveryRetentionUnavailable(RuntimeError):
    """Raised when a safety-critical outbox TTL migration cannot complete."""


def _ensure_partial_ttl(
    collection,
    *,
    legacy_name: str,
    target_name: str,
    expire_after: int,
    partial_filter: dict,
) -> None:
    info = collection.index_information()
    legacy = info.get(legacy_name)
    target = info.get(target_name)

    if legacy is not None:
        collection.drop_index(legacy_name)

    target_matches = (
        target is not None
        and target.get("expireAfterSeconds") == expire_after
        and target.get("partialFilterExpression") == partial_filter
    )
    if target is not None and not target_matches:
        collection.drop_index(target_name)
        target = None

    if target is None or not target_matches:
        collection.create_index(
            [("created_at_dt", ASCENDING)],
            expireAfterSeconds=expire_after,
            partialFilterExpression=partial_filter,
            name=target_name,
            background=True,
        )


def ensure_state_aware_delivery_ttl() -> bool:
    """Replace generic battle/report TTL indexes with delivered-only TTL indexes.

    Returns ``False`` only when neither collection exists (for example a local
    no-Mongo test process). If either configured collection cannot be migrated,
    the function fails closed instead of claiming pending delivery is protected.
    """
    import database

    battles = getattr(database, "battles_collection", None)
    reports = getattr(database, "reports_collection", None)
    if battles is None and reports is None:
        return False

    try:
        if battles is not None:
            _ensure_partial_ttl(
                battles,
                legacy_name=_BATTLE_LEGACY_TTL,
                target_name=_BATTLE_DELIVERED_TTL,
                expire_after=_BATTLE_RETENTION_SECONDS,
                partial_filter=_BATTLE_DELIVERED_FILTER,
            )
        if reports is not None:
            _ensure_partial_ttl(
                reports,
                legacy_name=_REPORT_LEGACY_TTL,
                target_name=_REPORT_DELIVERED_TTL,
                expire_after=_REPORT_RETENTION_SECONDS,
                partial_filter=_REPORT_DELIVERED_FILTER,
            )
        return True
    except PyMongoError as exc:
        logger.exception("failed to install state-aware delivery retention")
        raise DeliveryRetentionUnavailable(
            "battle/report delivery retention migration failed"
        ) from exc
