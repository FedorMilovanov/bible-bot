"""Read-only deployment preflight for duplicate open Mini App sessions.

Run before changing the Mini App open-session unique index. The command connects
directly to MongoDB and performs only ``ping`` + ``aggregate`` reads. It never
imports application database/bootstrap modules, mutates MongoDB, or chooses a
winner among contradictory session rows.
"""
from __future__ import annotations

import json
import os
import sys

from pymongo import MongoClient
from pymongo.errors import PyMongoError

_DB_NAME = "bible_bot_db"
_COLLECTION_NAME = "miniapp_sessions"
_SERVER_SELECTION_TIMEOUT_MS = 5000
_DEFAULT_LIMIT = 500
_OPEN_STATUSES = ("in_progress", "finalizing", "score_error")


class MiniAppDuplicatePreflightUnavailable(RuntimeError):
    """MongoDB Mini App duplicate-session state cannot be read safely."""


def _pipeline(limit: int) -> list[dict]:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    return [
        {"$match": {"status": {"$in": list(_OPEN_STATUSES)}}},
        {
            "$group": {
                "_id": "$user_id",
                "session_ids": {"$push": "$_id"},
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1, "_id": 1}},
        {"$limit": limit},
    ]


def _load_duplicates(limit: int = _DEFAULT_LIMIT) -> list[dict]:
    pipeline = _pipeline(limit)
    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        raise MiniAppDuplicatePreflightUnavailable("MONGO_URL is not configured")

    client = MongoClient(
        mongo_url,
        serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT_MS,
    )
    try:
        client.admin.command("ping")
        rows = client[_DB_NAME][_COLLECTION_NAME].aggregate(pipeline)
        return [
            {
                "user_id": row.get("_id"),
                "count": row.get("count"),
                "session_ids": list(row.get("session_ids") or []),
            }
            for row in rows
        ]
    except PyMongoError as exc:
        raise MiniAppDuplicatePreflightUnavailable(
            "Mini App duplicate-session preflight failed"
        ) from exc
    finally:
        client.close()


def main() -> int:
    try:
        duplicates = _load_duplicates()
    except MiniAppDuplicatePreflightUnavailable as exc:
        print(
            json.dumps(
                {"ok": False, "error": "preflight_unavailable", "detail": str(exc)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2

    if duplicates:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "duplicate_open_miniapp_sessions",
                    "count": len(duplicates),
                    "users": duplicates,
                },
                ensure_ascii=False,
            )
        )
        return 1

    print(json.dumps({"ok": True, "duplicate_open_miniapp_sessions": 0}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
