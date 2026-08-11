"""Read-only preflight for embedded result-receipt growth and Mongo topology.

Crash-safe scoring intentionally keeps result/bonus receipts non-evicting so old
replays cannot mint points again. Before redesigning that storage, measure real
user-document size and receipt-map growth and identify whether the Mongo topology
is a transaction-capable candidate. This command performs only ``hello``,
``ping`` and aggregation reads and never imports application database bootstrap.
"""
from __future__ import annotations

import json
import os
import sys

from pymongo import MongoClient
from pymongo.errors import PyMongoError

_DB_NAME = "bible_bot_db"
_COLLECTION_NAME = "leaderboard"
_SERVER_SELECTION_TIMEOUT_MS = 5000
_DEFAULT_LIMIT = 20
_BSON_MAX_BYTES = 16 * 1024 * 1024
_WARNING_BYTES = 12 * 1024 * 1024

_RECEIPT_MAPS = {
    "result_receipts": "legacy_result_receipts",
    "daily_bonus_receipts": "daily_bonus_receipts",
    "normal_bonus_owners": "normal_bonus_result_owners",
    "random20_bonus_receipts": "challenge_bonus_receipts.random20",
    "hardcore20_bonus_receipts": "challenge_bonus_receipts.hardcore20",
    "random20_bonus_owners": "challenge_bonus_result_owners.random20",
    "hardcore20_bonus_owners": "challenge_bonus_result_owners.hardcore20",
}


class ResultStoragePreflightUnavailable(RuntimeError):
    """MongoDB result-storage state cannot be inspected safely."""


def _map_count_expression(path: str) -> dict:
    field = f"${path}"
    field_type = {"$type": field}
    return {
        "$cond": [
            {"$in": [field_type, ["missing", "null"]]},
            0,
            {
                "$cond": [
                    {"$eq": [field_type, "object"]},
                    {"$size": {"$objectToArray": field}},
                    -1,
                ]
            },
        ]
    }


def _pipeline(limit: int) -> list[dict]:
    projection = {
        "_id": 1,
        "bson_size": {"$bsonSize": "$$ROOT"},
    }
    projection.update(
        {
            label: _map_count_expression(path)
            for label, path in _RECEIPT_MAPS.items()
        }
    )
    return [
        {"$project": projection},
        {"$sort": {"bson_size": -1, "_id": 1}},
        {"$limit": limit},
    ]


def _topology(hello: dict) -> str:
    if not isinstance(hello, dict):
        return "unknown"
    if hello.get("msg") == "isdbgrid":
        return "sharded"
    if isinstance(hello.get("setName"), str) and hello.get("setName"):
        return "replica_set"
    return "standalone"


def _load_storage_snapshot(limit: int = _DEFAULT_LIMIT) -> dict:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")

    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        raise ResultStoragePreflightUnavailable("MONGO_URL is not configured")

    client = MongoClient(
        mongo_url,
        serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT_MS,
    )
    try:
        client.admin.command("ping")
        hello = client.admin.command("hello")
        rows = list(
            client[_DB_NAME][_COLLECTION_NAME].aggregate(
                _pipeline(limit),
                allowDiskUse=False,
            )
        )
    except PyMongoError as exc:
        raise ResultStoragePreflightUnavailable(
            "result-storage growth preflight failed"
        ) from exc
    finally:
        client.close()

    topology = _topology(hello)
    users = []
    malformed_maps = 0
    for row in rows:
        if not isinstance(row, dict):
            raise ResultStoragePreflightUnavailable(
                "result-storage aggregation returned invalid row"
            )
        bson_size = row.get("bson_size")
        if isinstance(bson_size, bool) or not isinstance(bson_size, int) or bson_size < 0:
            raise ResultStoragePreflightUnavailable(
                "result-storage aggregation returned invalid BSON size"
            )
        counts = {}
        for label in _RECEIPT_MAPS:
            value = row.get(label, 0)
            if isinstance(value, bool) or not isinstance(value, int) or value < -1:
                raise ResultStoragePreflightUnavailable(
                    "result-storage aggregation returned invalid receipt count"
                )
            if value == -1:
                malformed_maps += 1
            counts[label] = value
        users.append(
            {
                "user_id": row.get("_id"),
                "bson_size": bson_size,
                "bson_usage_ratio": round(bson_size / _BSON_MAX_BYTES, 6),
                "warning": bson_size >= _WARNING_BYTES,
                **counts,
            }
        )

    return {
        "topology": topology,
        "transaction_topology_candidate": topology in {"replica_set", "sharded"},
        "bson_max_bytes": _BSON_MAX_BYTES,
        "warning_bytes": _WARNING_BYTES,
        "malformed_receipt_maps": malformed_maps,
        "users": users,
    }


def main() -> int:
    try:
        snapshot = _load_storage_snapshot()
    except ResultStoragePreflightUnavailable as exc:
        print(
            json.dumps(
                {"ok": False, "error": "preflight_unavailable", "detail": str(exc)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2

    print(json.dumps({"ok": True, **snapshot}, ensure_ascii=False))
    if snapshot["malformed_receipt_maps"]:
        return 1
    if any(user["warning"] for user in snapshot["users"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
