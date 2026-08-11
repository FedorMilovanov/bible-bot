"""Read-only deployment preflight for session uniqueness index contracts.

The command inspects only ``index_information()`` for the legacy Telegram and
Mini App session collections. It never imports application database/bootstrap
modules and never creates, drops, or modifies indexes.
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
        "uniq_active_quiz_user",
        [("user_id", 1)],
        True,
        {"status": "in_progress"},
    ),
    (
        "miniapp_sessions",
        "uniq_miniapp_active_user",
        [("user_id", 1)],
        True,
        {"status": {"$in": ["in_progress", "finalizing", "score_error"]}},
    ),
)


class SessionUniqueIndexPreflightUnavailable(RuntimeError):
    """MongoDB session uniqueness index state cannot be read safely."""


def _load_index_information() -> dict[str, dict]:
    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        raise SessionUniqueIndexPreflightUnavailable("MONGO_URL is not configured")

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
        raise SessionUniqueIndexPreflightUnavailable(
            "session unique-index lookup failed"
        ) from exc
    finally:
        client.close()


def _audit_collection(spec: tuple, info: dict, problems: list[dict]) -> None:
    collection, index_name, key, unique, partial = spec
    index = info.get(index_name)
    if index is None:
        problems.append(
            {
                "collection": collection,
                "error": "unique_session_index_missing",
                "index": index_name,
            }
        )
        return

    if index.get("key") != key:
        problems.append(
            {
                "collection": collection,
                "error": "unique_session_index_wrong_key",
                "index": index_name,
                "actual": index.get("key"),
                "expected": key,
            }
        )
    if bool(index.get("unique", False)) is not unique:
        problems.append(
            {
                "collection": collection,
                "error": "unique_session_index_not_unique",
                "index": index_name,
            }
        )
    if index.get("partialFilterExpression") != partial:
        problems.append(
            {
                "collection": collection,
                "error": "unique_session_index_wrong_filter",
                "index": index_name,
                "actual": index.get("partialFilterExpression"),
                "expected": partial,
            }
        )


def main() -> int:
    try:
        index_info = _load_index_information()
    except SessionUniqueIndexPreflightUnavailable as exc:
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
                {
                    "ok": False,
                    "error": "session_unique_indexes_unsafe",
                    "problems": problems,
                },
                ensure_ascii=False,
            )
        )
        return 1

    print(json.dumps({"ok": True, "session_unique_indexes": "safe"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
