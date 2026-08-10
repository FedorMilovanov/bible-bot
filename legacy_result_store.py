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


def _normalize_result_id(result_id: str) -> str:
    value = str(result_id or "").strip()
    if not value:
        raise ValueError("result_id is required for idempotent legacy scoring")
    return value


def _receipt_digest(result_id: str) -> str:
    value = _normalize_result_id(result_id)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _receipt_path(result_id: str) -> str:
    """Return a Mongo-safe, non-evicting field path for one result identity."""
    return f"legacy_result_receipts.{_receipt_digest(result_id)}"


def _receipt_snapshot(doc: dict, result_id: str) -> dict:
    """Return the durable per-result snapshot, including legacy string receipts."""
    receipts = doc.get("legacy_result_receipts", {})
    if not isinstance(receipts, dict):
        return {}
    value = receipts.get(_receipt_digest(result_id))
    if isinstance(value, dict):
        completed_at = value.get("completed_at")
        if isinstance(completed_at, datetime):
            completed_at = completed_at.isoformat()
        if not isinstance(completed_at, str) or not completed_at:
            return {}
        return {
            "completed_at": completed_at,
            "daily_streak": max(0, int(value.get("daily_streak", 0) or 0)),
            "challenge_streak": max(0, int(value.get("challenge_streak", 0) or 0)),
        }
    if isinstance(value, datetime):
        return {"completed_at": value.isoformat(), "daily_streak": 0, "challenge_streak": 0}
    if isinstance(value, str) and value:
        return {"completed_at": value, "daily_streak": 0, "challenge_streak": 0}
    return {}


def _receipt_completed_at(doc: dict, result_id: str) -> str | None:
    return _receipt_snapshot(doc, result_id).get("completed_at")


def _coerce_completed_at(value: str | datetime | float | int | None) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass
    if value is not None:
        try:
            return datetime.utcfromtimestamp(float(value))
        except (TypeError, ValueError, OSError, OverflowError):
            pass
    return datetime.utcnow()


def result_day(completed_at: str | datetime | float | int | None) -> str:
    """Return the UTC calendar day of the first durable base-result write."""
    return _coerce_completed_at(completed_at).strftime("%Y-%m-%d")


def _day_key(day: str) -> str:
    try:
        parsed = datetime.strptime(day, "%Y-%m-%d")
    except (TypeError, ValueError) as exc:
        raise ValueError("result day must be YYYY-MM-DD") from exc
    return parsed.strftime("%Y%m%d")


def _bonus_stage(receipt, *, claimed_now: bool) -> dict:
    """Normalize a durable bonus receipt into the public stage result."""
    if isinstance(receipt, dict):
        bonus = max(0, int(receipt.get("bonus", 0) or 0))
        eligible = bool(receipt.get("eligible", False))
    else:
        # Transitional compatibility with boolean receipts from development
        # checkpoints. No amount can be reconstructed from those markers.
        bonus = 0
        eligible = bool(receipt)
    return {
        "bonus": bonus,
        "eligible": eligible,
        "claimed_now": claimed_now,
    }


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

    if last != today:
        return {
            "challenge_streak_count": 0,
            "challenge_streak_last_date": today,
        }
    return {}


def apply_base_result_once(
    *,
    result_id: str,
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
    """Apply base quiz counters/points exactly once and persist retry snapshots."""
    result_id = _normalize_result_id(result_id)
    collection = _users()
    entry = _ensure_user(user_id, username, first_name)
    uid = database._uid(user_id)
    level_key = database._safe_level_key(level_key)
    score, total = database._validate_score(score, total)
    time_seconds = max(0.0, float(time_seconds))
    score_multiplier = max(0.0, float(score_multiplier))
    max_streak = max(0, int(max_streak))
    now = database._now_utc()
    completed_at = now.isoformat()
    today = now.strftime("%Y-%m-%d")

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

    daily_fields = _daily_activity_fields(entry, today)
    challenge_fields = _challenge_streak_fields(entry, today, challenge_mode, score)
    daily_streak_after = max(0, int(
        daily_fields.get("daily_activity_streak", entry.get("daily_activity_streak", 0)) or 0
    ))
    challenge_streak_after = max(0, int(
        challenge_fields.get("challenge_streak_count", entry.get("challenge_streak_count", 0)) or 0
    ))
    receipt_snapshot = {
        "completed_at": completed_at,
        "daily_streak": daily_streak_after,
        "challenge_streak": challenge_streak_after,
    }
    receipt_path = _receipt_path(result_id)
    set_fields = {
        "username": username or "",
        "first_name": first_name or "Пользователь",
        "last_activity": now,
        receipt_path: receipt_snapshot,
        **daily_fields,
        **challenge_fields,
    }
    if is_perfect:
        set_fields["last_perfect_date"] = today

    try:
        after = collection.find_one_and_update(
            {"_id": uid, receipt_path: {"$exists": False}},
            {
                "$inc": inc,
                "$set": set_fields,
                "$max": {
                    f"{level_key}_best_score": score,
                    "max_streak_ever": max_streak,
                },
            },
            return_document=ReturnDocument.AFTER,
        )
        if after is not None:
            return {
                "applied": True,
                "earned_base": earned_base,
                "completed_at": completed_at,
                "receipt": receipt_snapshot,
                "user": after,
            }

        existing = collection.find_one({"_id": uid, receipt_path: {"$exists": True}})
        if existing:
            durable_receipt = _receipt_snapshot(existing, result_id)
            durable_completed_at = durable_receipt.get("completed_at")
            if durable_completed_at is None:
                raise LegacyResultStoreUnavailable("base result receipt timestamp is invalid")
            return {
                "applied": False,
                "earned_base": earned_base,
                "completed_at": durable_completed_at,
                "receipt": durable_receipt,
                "user": existing,
            }
        raise LegacyResultStoreUnavailable("base result receipt could not be persisted")
    except LegacyResultStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("base result write failed") from exc


def _stored_bonus_amount(value) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def claim_daily_bonus_state(user_id: int, day: str, daily_streak: int) -> dict:
    """Claim or recover the normal-test bonus for the result's durable day."""
    collection = _users()
    uid = database._uid(user_id)
    day_key = _day_key(day)
    receipt_path = f"daily_bonus_receipts.{day_key}"
    daily_streak = max(0, int(daily_streak or 0))
    bonus = 15 if daily_streak >= 7 else 10 if daily_streak >= 3 else 5
    try:
        entry = collection.find_one({"_id": uid}, {"last_daily_bonus": 1, receipt_path: 1})
        if not entry:
            return {"bonus": 0, "claimed_now": False}
        receipt_value = entry.get("daily_bonus_receipts", {}).get(day_key)
        if receipt_value is not None:
            return {"bonus": _stored_bonus_amount(receipt_value), "claimed_now": False}
        if entry.get("last_daily_bonus", "") == day:
            collection.update_one(
                {"_id": uid, receipt_path: {"$exists": False}},
                {"$set": {receipt_path: 0}},
            )
            return {"bonus": 0, "claimed_now": False}

        result = collection.update_one(
            {"_id": uid, receipt_path: {"$exists": False}},
            {
                "$set": {receipt_path: bonus},
                "$max": {"last_daily_bonus": day},
                "$inc": {"total_points": bonus},
            },
        )
        if result.modified_count == 1:
            return {"bonus": bonus, "claimed_now": True}
        raced = collection.find_one({"_id": uid}, {receipt_path: 1}) or {}
        raced_value = raced.get("daily_bonus_receipts", {}).get(day_key)
        return {"bonus": _stored_bonus_amount(raced_value), "claimed_now": False}
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("daily bonus write failed") from exc


def claim_daily_bonus_once(user_id: int, day: str, daily_streak: int | None = None) -> int:
    """Backward-compatible wrapper returning points only for the winning claim."""
    if daily_streak is None:
        entry = _users().find_one({"_id": database._uid(user_id)}, {"daily_activity_streak": 1}) or {}
        daily_streak = int(entry.get("daily_activity_streak", 0) or 0)
    state = claim_daily_bonus_state(user_id, day, daily_streak)
    return state["bonus"] if state["claimed_now"] else 0


def claim_challenge_bonus_state(user_id: int, mode: str, score: int, day: str) -> dict:
    """Claim or recover a Challenge bonus for the result's durable day."""
    if mode not in _CHALLENGE_MODES:
        raise ValueError(f"unsupported challenge mode: {mode}")
    collection = _users()
    uid = database._uid(user_id)
    day_key = _day_key(day)
    score, _ = database._validate_score(score, 20)
    bonus = database.compute_bonus(score, mode, True)
    date_field = f"{mode}_last_bonus_date"
    receipt_path = f"challenge_bonus_receipts.{mode}.{day_key}"
    try:
        entry = collection.find_one({"_id": uid}, {date_field: 1, receipt_path: 1})
        if not entry:
            return {"bonus": 0, "claimed_now": False}
        receipt_value = entry.get("challenge_bonus_receipts", {}).get(mode, {}).get(day_key)
        if receipt_value is not None:
            return {"bonus": _stored_bonus_amount(receipt_value), "claimed_now": False}
        if entry.get(date_field, "") == day:
            collection.update_one(
                {"_id": uid, receipt_path: {"$exists": False}},
                {"$set": {receipt_path: 0}},
            )
            return {"bonus": 0, "claimed_now": False}

        update: dict = {
            "$set": {receipt_path: bonus},
            "$max": {date_field: day},
        }
        if bonus:
            update["$inc"] = {"total_points": bonus}
        result = collection.update_one(
            {"_id": uid, receipt_path: {"$exists": False}},
            update,
        )
        if result.modified_count == 1:
            return {"bonus": bonus, "claimed_now": True}
        raced = collection.find_one({"_id": uid}, {receipt_path: 1}) or {}
        raced_value = raced.get("challenge_bonus_receipts", {}).get(mode, {}).get(day_key)
        return {"bonus": _stored_bonus_amount(raced_value), "claimed_now": False}
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("challenge bonus write failed") from exc


def claim_challenge_bonus_once(user_id: int, mode: str, score: int, day: str) -> int:
    """Backward-compatible wrapper returning points only for the winning claim."""
    state = claim_challenge_bonus_state(user_id, mode, score, day)
    return state["bonus"] if state["claimed_now"] else 0


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


def result_week_id(completed_at: str | datetime | float | int | None) -> str:
    """Return the UTC ISO week of the first durable base-result write."""
    iso = _coerce_completed_at(completed_at).isocalendar()
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
