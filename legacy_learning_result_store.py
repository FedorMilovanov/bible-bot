"""Idempotent progress persistence for non-scoring Telegram learning courses.

Learning-only courses record course progress but never touch ranking totals,
daily bonuses, achievements, Challenge, or Battle state. A per-attempt receipt
makes the progress update retry-safe if process/session finalization fails after
the user document was already updated.
"""
from __future__ import annotations

import hashlib
import re

from pymongo.errors import PyMongoError

import database
from questions.pool_policy import is_non_scoring_learning_pool

_LEVEL_KEY_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class LegacyLearningProgressUnavailable(RuntimeError):
    """Learning progress could not be proven/persisted safely."""


def _receipt_digest(result_id: str) -> str:
    value = str(result_id or "").strip()
    if not value:
        raise ValueError("result_id is required")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _receipt_path(result_id: str) -> str:
    return f"legacy_learning_receipts.{_receipt_digest(result_id)}"


def _nested_value(document: dict, dotted_path: str) -> tuple[object | None, bool]:
    current: object = document
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current[part]
    return current, True


def _score_pair(score: int, total: int) -> tuple[int, int]:
    if (
        isinstance(score, bool)
        or not isinstance(score, int)
        or isinstance(total, bool)
        or not isinstance(total, int)
        or total < 1
        or total > 100
        or score < 0
        or score > total
    ):
        raise ValueError("learning score/total is invalid")
    return score, total


def _validated_existing_receipt(
    stored: object,
    *,
    level_key: str,
    score: int,
    total: int,
) -> dict:
    if not isinstance(stored, dict) or stored.get("kind") != "learning":
        raise LegacyLearningProgressUnavailable("learning receipt is invalid")
    expected = {
        "level_key": level_key,
        "score": score,
        "total": total,
        "points": 0,
        "daily_bonus": 0,
    }
    if any(stored.get(key) != value for key, value in expected.items()):
        raise LegacyLearningProgressUnavailable(
            "learning receipt does not match the durable result being retried"
        )
    achievements = stored.get("new_achievements")
    if achievements != []:
        raise LegacyLearningProgressUnavailable("learning receipt contains scoring side effects")
    return stored


def apply_learning_progress_once(
    *,
    result_id: str,
    user_id: int,
    username: str,
    first_name: str,
    level_key: str,
    score: int,
    total: int,
) -> dict:
    """Apply one non-scoring course progress result exactly once."""
    if not isinstance(level_key, str) or not _LEVEL_KEY_RE.fullmatch(level_key):
        raise ValueError("unsafe learning level_key")
    if not is_non_scoring_learning_pool(level_key):
        raise ValueError("learning progress store requires a non-scoring pool")
    score, total = _score_pair(score, total)
    receipt_path = _receipt_path(result_id)

    collection = getattr(database, "collection", None)
    if collection is None:
        raise LegacyLearningProgressUnavailable("user stats collection is unavailable")

    uid = database._uid(user_id)
    try:
        existing = collection.find_one({"_id": uid})
        if existing is None:
            database.init_user_stats(user_id, username or "", first_name or "Пользователь")
            existing = collection.find_one({"_id": uid})
        if existing is None:
            raise LegacyLearningProgressUnavailable("user stats document is unavailable")

        stored, exists = _nested_value(existing, receipt_path)
        if exists:
            validated = _validated_existing_receipt(
                stored,
                level_key=level_key,
                score=score,
                total=total,
            )
            return {**validated, "applied": False}

        now = database._now_utc()
        receipt = {
            "kind": "learning",
            "level_key": level_key,
            "score": score,
            "total": total,
            "points": 0,
            "daily_bonus": 0,
            "new_achievements": [],
            "applied_at": now,
        }
        result = collection.update_one(
            {"_id": uid, receipt_path: {"$exists": False}},
            {
                "$inc": {
                    f"{level_key}_attempts": 1,
                    f"{level_key}_correct": score,
                    f"{level_key}_total": total,
                },
                "$set": {
                    "username": username or "",
                    "first_name": first_name or "Пользователь",
                    "last_activity": now,
                    receipt_path: receipt,
                },
                "$max": {f"{level_key}_best_score": score},
            },
        )
        if getattr(result, "modified_count", 0) == 1:
            return {**receipt, "applied": True}

        refreshed = collection.find_one({"_id": uid})
        stored, exists = _nested_value(refreshed or {}, receipt_path)
        if exists:
            validated = _validated_existing_receipt(
                stored,
                level_key=level_key,
                score=score,
                total=total,
            )
            return {**validated, "applied": False}
        raise LegacyLearningProgressUnavailable(
            "learning progress was not applied and no prior receipt exists"
        )
    except LegacyLearningProgressUnavailable:
        raise
    except PyMongoError as exc:
        raise LegacyLearningProgressUnavailable("learning progress write failed") from exc
