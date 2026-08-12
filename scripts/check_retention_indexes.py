"""Read-only deployment preflight for durable-evidence retention indexes.

The command never imports the application database bootstrap and never creates,
drops, or modifies an index. It connects directly to MongoDB, reads
``index_information()``, and verifies the durable-evidence retention contract.
"""
from __future__ import annotations

import json
import os
import sys

from pymongo import MongoClient
from pymongo.errors import PyMongoError

_DB_NAME = "bible_bot_db"
_SERVER_SELECTION_TIMEOUT_MS = 5000

EXPECTED = (
    (
        "quiz_sessions",
        "ttl_updated_at",
        "ttl_terminal_updated_at",
        [("updated_at_dt", 1)],
        90 * 24 * 60 * 60,
        {"status": {"$in": ["finished", "cancelled"]}},
    ),
    (
        "miniapp_sessions",
        "ttl_miniapp_updated_at",
        "ttl_miniapp_terminal_updated_at",
        [("updated_at_dt", 1)],
        90 * 24 * 60 * 60,
        {"status": {"$in": ["finished", "abandoned"]}},
    ),
    (
        "battles",
        "ttl_battles_created_at",
        "ttl_battles_delivered_created_at",
        [("created_at_dt", 1)],
        30 * 24 * 60 * 60,
        {
            "status": "finalized",
            "result_delivery.creator.delivered": True,
            "result_delivery.opponent.delivered": True,
        },
    ),
    (
        "reports",
        "ttl_reports_created_at",
        "ttl_reports_delivered_created_at",
        [("created_at_dt", 1)],
        90 * 24 * 60 * 60,
        {"admin_delivered": True},
    ),
    (
        "broadcasts",
        "ttl_broadcast_created_at",
        "ttl_broadcast_retention",
        [("retention_at_dt", 1)],
        90 * 24 * 60 * 60,
        None,
    ),
    (
        "broadcast_deliveries",
        "ttl_broadcast_delivery_created_at",
        "ttl_broadcast_delivery_retention",
        [("retention_at_dt", 1)],
        90 * 24 * 60 * 60,
        None,
    ),
)


class RetentionPreflightUnavailable(RuntimeError):
    """MongoDB retention state cannot be read safely."""


def _load_index_information() -> dict[str, dict]:
    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        raise RetentionPreflightUnavailable("MONGO_URL is not configured")

    client = MongoClient(
        mongo_url,
        serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT_MS,
    )
    try:
        client.admin.command("ping")
        db = client[_DB_NAME]
        return {
            collection_name: db[collection_name].index_information()
            for collection_name, *_rest in EXPECTED
        }
    except PyMongoError as exc:
        raise RetentionPreflightUnavailable("MongoDB index lookup failed") from exc
    finally:
        client.close()


def _audit_collection(spec: tuple, info: dict, problems: list[dict]) -> None:
    label, legacy_name, target_name, key, expire_after, partial = spec

    if legacy_name in info:
        problems.append(
            {
                "collection": label,
                "error": "unsafe_legacy_ttl_present",
                "index": legacy_name,
            }
        )

    target = info.get(target_name)
    if target is None:
        problems.append(
            {
                "collection": label,
                "error": "state_aware_ttl_missing",
                "index": target_name,
            }
        )
        return

    if target.get("key") != key:
        problems.append(
            {
                "collection": label,
                "error": "state_aware_ttl_wrong_key",
                "index": target_name,
                "actual": target.get("key"),
                "expected": key,
            }
        )
    if target.get("expireAfterSeconds") != expire_after:
        problems.append(
            {
                "collection": label,
                "error": "state_aware_ttl_wrong_expiry",
                "index": target_name,
                "actual": target.get("expireAfterSeconds"),
                "expected": expire_after,
            }
        )
    if target.get("partialFilterExpression") != partial:
        problems.append(
            {
                "collection": label,
                "error": "state_aware_ttl_wrong_filter",
                "index": target_name,
            }
        )


def main() -> int:
    try:
        index_info = _load_index_information()
    except RetentionPreflightUnavailable as exc:
        print(
            json.dumps(
                {"ok": False, "error": "preflight_unavailable", "detail": str(exc)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2

    problems: list[dict] = []
    for spec in EXPECTED:
        _audit_collection(spec, index_info.get(spec[0], {}), problems)

    if problems:
        print(
            json.dumps(
                {"ok": False, "error": "retention_indexes_unsafe", "problems": problems},
                ensure_ascii=False,
            )
        )
        return 1

    print(json.dumps({"ok": True, "retention_indexes": "safe"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
