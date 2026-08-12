"""Fail-closed Mongo index bootstrap for durable broadcast state.

Production must not preserve an age-based or otherwise unrecognized TTL on
unfinished broadcast evidence. This module only creates missing known-safe
indexes. It never drops or rewrites an existing index contract.
"""
from __future__ import annotations

import logging

from pymongo import ASCENDING
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

_RETENTION_SECONDS = 90 * 24 * 60 * 60

_BROADCAST_TTL = (
    "ttl_broadcast_retention",
    [("retention_at_dt", ASCENDING)],
    _RETENTION_SECONDS,
)
_DELIVERY_TTL = (
    "ttl_broadcast_delivery_retention",
    [("retention_at_dt", ASCENDING)],
    _RETENTION_SECONDS,
)

_LOOKUP_INDEXES = (
    (
        "broadcasts",
        "idx_broadcast_pending",
        [("completed", ASCENDING), ("created_at_dt", ASCENDING)],
    ),
    (
        "broadcast_deliveries",
        "idx_broadcast_delivery_claim",
        [("done", ASCENDING), ("lease_until", ASCENDING), ("created_at_dt", ASCENDING)],
    ),
    (
        "broadcast_deliveries",
        "idx_broadcast_delivery_parent",
        [("broadcast_id", ASCENDING), ("done", ASCENDING)],
    ),
)


class BroadcastIndexSafetyUnavailable(RuntimeError):
    """Raised when the production broadcast index contract cannot be proven safe."""


def _database():
    import database

    db = getattr(database, "db", None)
    if db is None:
        raise BroadcastIndexSafetyUnavailable("broadcast database is unavailable")
    return db


def _ttl_exact(options: dict, *, key: list[tuple], expire_after: int) -> bool:
    return (
        isinstance(options, dict)
        and options.get("key") == key
        and options.get("expireAfterSeconds") == expire_after
        and options.get("partialFilterExpression") is None
    )


def _audit_ttl_contract(
    *,
    collection_name: str,
    info: dict,
    expected_name: str,
    expected_key: list[tuple],
    expire_after: int,
    allow_missing: bool,
) -> None:
    if not isinstance(info, dict):
        raise BroadcastIndexSafetyUnavailable(
            f"{collection_name} index metadata is malformed"
        )

    for index_name, options in info.items():
        if (
            isinstance(options, dict)
            and "expireAfterSeconds" in options
            and index_name != expected_name
        ):
            raise BroadcastIndexSafetyUnavailable(
                f"{collection_name} contains unrecognized TTL index {index_name}"
            )

    target = info.get(expected_name)
    if target is None:
        if allow_missing:
            return
        raise BroadcastIndexSafetyUnavailable(
            f"{collection_name} required TTL index {expected_name} is missing"
        )
    if not _ttl_exact(target, key=expected_key, expire_after=expire_after):
        raise BroadcastIndexSafetyUnavailable(
            f"{collection_name} TTL index {expected_name} is incompatible"
        )


def _audit_lookup_contract(
    *,
    collection_name: str,
    info: dict,
    expected_name: str,
    expected_key: list[tuple],
    allow_missing: bool,
) -> None:
    target = info.get(expected_name) if isinstance(info, dict) else None
    if target is None:
        if allow_missing:
            return
        raise BroadcastIndexSafetyUnavailable(
            f"{collection_name} required index {expected_name} is missing"
        )
    if not isinstance(target, dict) or target.get("key") != expected_key:
        raise BroadcastIndexSafetyUnavailable(
            f"{collection_name} index {expected_name} is incompatible"
        )
    if target.get("unique") is True or target.get("partialFilterExpression") is not None:
        raise BroadcastIndexSafetyUnavailable(
            f"{collection_name} index {expected_name} has incompatible options"
        )


def ensure_broadcast_indexes() -> None:
    """Prove/create the exact non-destructive broadcast index contract."""
    db = _database()
    broadcasts = db["broadcasts"]
    deliveries = db["broadcast_deliveries"]
    collections = {
        "broadcasts": broadcasts,
        "broadcast_deliveries": deliveries,
    }

    try:
        initial = {
            name: collection.index_information()
            for name, collection in collections.items()
        }
        _audit_ttl_contract(
            collection_name="broadcasts",
            info=initial["broadcasts"],
            expected_name=_BROADCAST_TTL[0],
            expected_key=_BROADCAST_TTL[1],
            expire_after=_BROADCAST_TTL[2],
            allow_missing=True,
        )
        _audit_ttl_contract(
            collection_name="broadcast_deliveries",
            info=initial["broadcast_deliveries"],
            expected_name=_DELIVERY_TTL[0],
            expected_key=_DELIVERY_TTL[1],
            expire_after=_DELIVERY_TTL[2],
            allow_missing=True,
        )
        for collection_name, index_name, key in _LOOKUP_INDEXES:
            _audit_lookup_contract(
                collection_name=collection_name,
                info=initial[collection_name],
                expected_name=index_name,
                expected_key=key,
                allow_missing=True,
            )

        if _BROADCAST_TTL[0] not in initial["broadcasts"]:
            broadcasts.create_index(
                _BROADCAST_TTL[1],
                name=_BROADCAST_TTL[0],
                expireAfterSeconds=_BROADCAST_TTL[2],
            )
        if _DELIVERY_TTL[0] not in initial["broadcast_deliveries"]:
            deliveries.create_index(
                _DELIVERY_TTL[1],
                name=_DELIVERY_TTL[0],
                expireAfterSeconds=_DELIVERY_TTL[2],
            )
        for collection_name, index_name, key in _LOOKUP_INDEXES:
            if index_name not in initial[collection_name]:
                collections[collection_name].create_index(key, name=index_name)

        final = {
            name: collection.index_information()
            for name, collection in collections.items()
        }
        _audit_ttl_contract(
            collection_name="broadcasts",
            info=final["broadcasts"],
            expected_name=_BROADCAST_TTL[0],
            expected_key=_BROADCAST_TTL[1],
            expire_after=_BROADCAST_TTL[2],
            allow_missing=False,
        )
        _audit_ttl_contract(
            collection_name="broadcast_deliveries",
            info=final["broadcast_deliveries"],
            expected_name=_DELIVERY_TTL[0],
            expected_key=_DELIVERY_TTL[1],
            expire_after=_DELIVERY_TTL[2],
            allow_missing=False,
        )
        for collection_name, index_name, key in _LOOKUP_INDEXES:
            _audit_lookup_contract(
                collection_name=collection_name,
                info=final[collection_name],
                expected_name=index_name,
                expected_key=key,
                allow_missing=False,
            )
    except BroadcastIndexSafetyUnavailable:
        raise
    except PyMongoError as exc:
        logger.exception("broadcast index safety bootstrap failed")
        raise BroadcastIndexSafetyUnavailable(
            "broadcast index safety bootstrap is unavailable"
        ) from exc
