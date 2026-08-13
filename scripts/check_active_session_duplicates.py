"""Read-only deployment preflight for duplicate active quiz sessions.

Run before enabling the strict one-active-session unique index. The command
connects directly to MongoDB and only performs ``ping`` + ``aggregate`` reads;
it never imports application database bootstrap code, mutates MongoDB, or
chooses a "winner" among contradictory legacy rows.
"""
from __future__ import annotations

import json
import os
import sys

from pymongo import MongoClient
from pymongo.errors import PyMongoError

_DB_NAME = "bible_bot_db"
_COLLECTION_NAME = "quiz_sessions"
_SERVER_SELECTION_TIMEOUT_MS = 5000
_DEFAULT_LIMIT = 500


class DuplicateSessionPreflightUnavailable(RuntimeError):
    """MongoDB duplicate-session state cannot be read safely."""


def _load_duplicates(limit: int = _DEFAULT_LIMIT) -> list[dict]:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")

    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        raise DuplicateSessionPreflightUnavailable("MONGO_URL is not configured")

    client = MongoClient(
        mongo_url,
        serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT_MS,
    )
    try:
        client.admin.command("ping")
        rows = client[_DB_NAME][_COLLECTION_NAME].aggregate(
            [
                {"$match": {"status": "in_progress"}},
                {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
                {"$match": {"count": {"$gt": 1}}},
                {"$sort": {"count": -1, "_id": 1}},
                {"$limit": limit},
            ]
        )
        return [
            {"user_id": row.get("_id"), "count": row.get("count")}
            for row in rows
        ]
    except PyMongoError as exc:
        raise DuplicateSessionPreflightUnavailable(
            "active-session duplicate preflight failed"
        ) from exc
    finally:
        client.close()


def main() -> int:
    try:
        duplicates = _load_duplicates()
    except DuplicateSessionPreflightUnavailable as exc:
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
                    "error": "duplicate_active_sessions",
                    "count": len(duplicates),
                    "users": duplicates,
                },
                ensure_ascii=False,
            )
        )
        return 1

    print(json.dumps({"ok": True, "duplicate_active_sessions": 0}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
