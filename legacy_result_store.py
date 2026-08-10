"""Crash-safe persistence primitives for legacy Telegram quiz results.

The legacy bot historically wrote one completed result through several independent
Mongo operations. These helpers provide stable per-session receipts and
idempotent follow-up claims so a process crash can safely retry finalization.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

import database

logger = logging.getLogger(__name__)

_CHALLENGE_MODES = frozenset({"random20", "hardcore20"})


class LegacyResultStoreUnavailable(RuntimeError):
    """Raised when a result stage cannot be made durable in MongoDB."""


def _users():
    collection = getattr(database, "collection", None)
    if collection is None:
        raise LegacyResultStoreUnavailable("user stats collection is unavailable")
    return collection


def _weekly():
    collection = getattr(database, "weekly_lb_collection", None)
    if collection is None:
        raise LegacyResultStoreUnavailable("weekly leaderboard collection is unavailable")
    return collection


def _receipt_path(result_id: str) -> str:
    """Return a Mongo-safe, non-evicting field path for one result identity."""
    digest = hashlib.sha256(str(result_id).encode("utf-8")).hexdigest()
    return f"legacy_result_receipts.{digest}"


def _ensure_user(user_id: int, username: str, first_name: str) -> dict:
    collection = _users()
    uid = database._uid(user_id)
    try:
        entry = collection.find_one({"_id": uid})
        if entry is None:
            database.init_user_stats(user_id, username or "", first_name or "Пользователь")
            entry = collection.find_one({"_id": uid})
        if entry is None:
            raise LegacyResultStoreUnavailable("user stats document could not be initialized")
        return entry
    except LegacyResultStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("user stats lookup failed") from exc


def _daily_activity_fields(entry: dict, today: str) -> dict:
    last_activity = entry.get("daily_activity_last", "")
    if last_activity == today:
        return {}

    previous = int(entry.get("daily_activity_streak", 0) or 0)
    if not last_activity:
        streak = 1
    else:
        try:
            last_dt = datetime.strptime(last_activity, "%Y-%m-%d")
            today_dt = datetime.strptime(today, "%Y-%m-%d")
            streak = previous + 1 if (today_dt - last_dt).days == 1 else 1
        except (TypeError, ValueError):
            streak = 1
    return {
        "daily_activity_streak": streak,
        "daily_activity_last": today,
    }


def _challenge_streak_fields(entry: dict, today: str, mode: str | None, score: int) -> dict:
    if mode not in _CHALLENGE_MODES:
        return {}

    last = entry.get("challenge_streak_last_date", "")
    current = int(entry.get("challenge_streak_count", 0) or 0)
    if score >= 18:
        if not last:
            streak = 1
        else:
            try:
                delta = (
                    datetime.strptime(today, "%Y-%m-%d")
                    - datetime.strptime(last, "%Y-%m-%d")
                ).days
                if delta == 1:
                    streak = current + 1
                elif delta == 0:
                    streak = current
                else:
                    streak = 1
            except (TypeError, ValueError):
                streak = 1
        return {
            "challenge_streak_count": streak,
            "challenge_streak_last_date": today,
        }

    # A failed attempt resets the streak only on the first challenge result of
    # the day, matching the legacy semantics.
    if last != today:
        return {
            "challenge_streak_count": 0,
            "challenge_streak_last_date": today,
        }
    return {}


def apply_base_result_once(
    *,
    result_id: str | None,
    user_id: int,
    username: str,
    first_name: str,
    level_key: str,
    score: int,
    total: int,
    time_seconds: float,
    score_multiplier: float = 1.0,
    max_streak: int = 0,
    challenge_mode: str | None = None,
) -> dict:
    """Apply base quiz counters/points once and return the durable user snapshot.

    A persisted quiz uses its session id as ``result_id``. Result markers are
    stored as hashed subdocument fields on the same user document as the
    counters, so the receipt and all ``$inc`` operations commit atomically and
    old receipts are never evicted by newer quiz results.

    If no result id exists, the current in-memory result can still be credited,
    but it has no cross-process retry identity. Callers should provide a stable
    in-memory fallback id whenever possible.
    """
    collection = _users()
    entry = _ensure_user(user_id, username, first_name)
    uid = database._uid(user_id)
    level_key = database._safe_level_key(level_key)
    score, total = database._validate_score(score, total)
    time_seconds = max(0.0, float(time_seconds))
    score_multiplier = max(0.0, float(score_multiplier))
    max_streak = max(0, int(max_streak))
    today = database._today_utc()
    now = database._now_utc()

    ppq = database.POINTS_PER_QUESTION.get(level_key, 1)
    earned_base = round(score * ppq * score_multiplier)
    is_perfect = total > 0 and score == total

    inc = {
        "total_tests": 1,
        "total_questions_answered": total,
        "total_correct_answers": score,
        "total_time_spent": time_seconds,
        "total_points": earned_base,
        f"{level_key}_attempts": 1,
        f"{level_key}_correct": score,
        f"{level_key}_total": total,
    }
    if is_perfect:
        inc["perfect_count"] = 1

    set_fields = {
        "username": username or "",
        "first_name": first_name or "Пользователь",
        "last_activity": now,
        **_daily_activity_fields(entry, today),
        **_challenge_streak_fields(entry, today, challenge_mode, score),
    }
    if is_perfect:
        set_fields["last_perfect_date"] = today

    update = {
        "$inc": inc,
        "$set": set_fields,
        "$max": {
            f"{level_key}_best_score": score,
            "max_streak_ever": max_streak,
        },
    }
    query = {"_id": uid}
    receipt_path = None
    if result_id:
        receipt_path = _receipt_path(result_id)
        query[receipt_path] = {"$exists": False}
        update["$set"][receipt_path] = now

    try:
        after = collection.find_one_and_update(
            query,
            update,
            return_document=ReturnDocument.AFTER,
        )
        if after is not None:
            return {
                "applied": True,
                "earned_base": earned_base,
                "user": after,
            }

        if receipt_path:
            existing = collection.find_one({"_id": uid, receipt_path: {"$exists": True}})
            if existing:
                return {
                    "applied": False,
                    "earned_base": earned_base,
                    "user": existing,
                }
        raise LegacyResultStoreUnavailable("base result receipt could not be persisted")
    except LegacyResultStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("base result write failed") from exc


def claim_daily_bonus_once(user_id: int) -> int:
    """Atomically award the first normal-test daily bonus of the UTC day."""
    collection = _users()
    uid = database._uid(user_id)
    today = database._today_utc()
    try:
        entry = collection.find_one(
            {"_id": uid},
            {"daily_activity_streak": 1, "last_daily_bonus": 1},
        )
        if not entry or entry.get("last_daily_bonus", "") == today:
            return 0
        streak = int(entry.get("daily_activity_streak", 0) or 0)
        bonus = 15 if streak >= 7 else 10 if streak >= 3 else 5
        result = collection.update_one(
            {"_id": uid, "last_daily_bonus": {"$ne": today}},
            {"$set": {"last_daily_bonus": today}, "$inc": {"total_points": bonus}},
        )
        return bonus if result.modified_count == 1 else 0
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("daily bonus write failed") from exc


def claim_challenge_bonus_once(user_id: int, mode: str, score: int) -> int:
    """Atomically consume and award the mode's once-per-day challenge bonus."""
    if mode not in _CHALLENGE_MODES:
        raise ValueError(f"unsupported challenge mode: {mode}")
    collection = _users()
    uid = database._uid(user_id)
    today = database._today_utc()
    score, _ = database._validate_score(score, 20)
    bonus = database.compute_bonus(score, mode, True)
    date_field = f"{mode}_last_bonus_date"
    update: dict = {"$set": {date_field: today}}
    if bonus:
        update["$inc"] = {"total_points": bonus}
    try:
        result = collection.update_one(
            {"_id": uid, date_field: {"$ne": today}},
            update,
        )
        return bonus if result.modified_count == 1 else 0
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("challenge bonus write failed") from exc


def claim_achievement_once(
    user_id: int,
    achievement_key: str,
    *,
    reward: int = 0,
    awarded_at: str | None = None,
) -> bool:
    """Claim an achievement/reward once using the achievement key itself as receipt."""
    if not achievement_key or "." in achievement_key or achievement_key.startswith("$"):
        raise ValueError("unsafe achievement key")
    collection = _users()
    uid = database._uid(user_id)
    awarded_at = awarded_at or datetime.now().strftime("%d.%m.%Y")
    update: dict = {"$set": {f"achievements.{achievement_key}": awarded_at}}
    reward = max(0, int(reward))
    if reward:
        update["$inc"] = {"total_points": reward}
    try:
        result = collection.update_one(
            {"_id": uid, f"achievements.{achievement_key}": {"$exists": False}},
            update,
        )
        return result.modified_count == 1
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("achievement claim failed") from exc


def result_week_id(start_time: float | int | None) -> str:
    """Return the UTC ISO week of the original quiz start for retry-stable weekly sync."""
    try:
        dt = datetime.utcfromtimestamp(float(start_time)) if start_time is not None else datetime.utcnow()
    except (TypeError, ValueError, OSError, OverflowError):
        dt = datetime.utcnow()
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def sync_weekly_best(
    *,
    user_id: int,
    username: str,
    first_name: str,
    mode: str,
    score: int,
    time_seconds: float,
    week_id: str,
) -> None:
    """Idempotently sync a Challenge best score into the originating ISO week."""
    if mode not in _CHALLENGE_MODES:
        raise ValueError(f"unsupported challenge mode: {mode}")
    collection = _weekly()
    score, _ = database._validate_score(score, 20)
    time_seconds = max(0.0, float(time_seconds))
    doc_id = f"{week_id}_{mode}_{user_id}"
    now = database._now_utc()
    replacement = {
        "week_id": week_id,
        "mode": mode,
        "user_id": database._uid(user_id),
        "username": username or "",
        "first_name": first_name or "Пользователь",
        "best_score": score,
        "best_time": time_seconds,
        "updated_at": now.isoformat(),
        "updated_at_dt": now,
    }

    try:
        existing = collection.find_one({"_id": doc_id})
        if existing is None:
            try:
                collection.insert_one({"_id": doc_id, **replacement})
                return
            except DuplicateKeyError:
                existing = collection.find_one({"_id": doc_id})

        if existing is None:
            raise LegacyResultStoreUnavailable("weekly result document disappeared")
        better = score > int(existing.get("best_score", 0) or 0)
        tied_faster = (
            score == int(existing.get("best_score", 0) or 0)
            and time_seconds < float(existing.get("best_time", 999999) or 999999)
        )
        if not (better or tied_faster):
            return

        collection.update_one(
            {
                "_id": doc_id,
                "$or": [
                    {"best_score": {"$lt": score}},
                    {"best_score": score, "best_time": {"$gt": time_seconds}},
                ],
            },
            {"$set": replacement},
        )
    except LegacyResultStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("weekly leaderboard sync failed") from exc
