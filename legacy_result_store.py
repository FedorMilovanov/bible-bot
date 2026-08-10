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
_BASE_CAS_RETRIES = 8


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
    return hashlib.sha256(_normalize_result_id(result_id).encode("utf-8")).hexdigest()


def _receipt_path(result_id: str) -> str:
    return f"legacy_result_receipts.{_receipt_digest(result_id)}"


def _receipt_snapshot(doc: dict, result_id: str) -> dict:
    """Return the durable per-result snapshot, including older receipt shapes."""
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
        result = value.get("result") if isinstance(value.get("result"), dict) else {}
        achievement_state = (
            value.get("achievement_state")
            if isinstance(value.get("achievement_state"), dict)
            else {}
        )
        return {
            "completed_at": completed_at,
            "daily_streak": max(0, int(value.get("daily_streak", 0) or 0)),
            "challenge_streak": max(0, int(value.get("challenge_streak", 0) or 0)),
            "result": dict(result),
            "achievement_state": dict(achievement_state),
        }
    if isinstance(value, datetime):
        return {
            "completed_at": value.isoformat(),
            "daily_streak": 0,
            "challenge_streak": 0,
            "result": {},
            "achievement_state": {},
        }
    if isinstance(value, str) and value:
        return {
            "completed_at": value,
            "daily_streak": 0,
            "challenge_streak": 0,
            "result": {},
            "achievement_state": {},
        }
    return {}


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
    return _coerce_completed_at(completed_at).strftime("%Y-%m-%d")


def result_week_id(completed_at: str | datetime | float | int | None) -> str:
    iso = _coerce_completed_at(completed_at).isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _day_key(day: str) -> str:
    try:
        parsed = datetime.strptime(day, "%Y-%m-%d")
    except (TypeError, ValueError) as exc:
        raise ValueError("result day must be YYYY-MM-DD") from exc
    return parsed.strftime("%Y%m%d")


def _bonus_stage(receipt, *, claimed_now: bool) -> dict:
    """Normalize a durable bonus receipt into recovery/UI state."""
    if isinstance(receipt, dict):
        bonus = max(0, int(receipt.get("bonus", 0) or 0))
        eligible = bool(receipt.get("eligible", False))
    elif isinstance(receipt, bool):
        bonus = 0
        eligible = receipt
    else:
        try:
            bonus = max(0, int(receipt or 0))
        except (TypeError, ValueError):
            bonus = 0
        eligible = bonus > 0
    return {"bonus": bonus, "eligible": eligible, "claimed_now": claimed_now}


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
            delta = (
                datetime.strptime(today, "%Y-%m-%d")
                - datetime.strptime(last_activity, "%Y-%m-%d")
            ).days
            streak = previous + 1 if delta == 1 else 1
        except (TypeError, ValueError):
            streak = 1
    return {"daily_activity_streak": streak, "daily_activity_last": today}


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
        return {"challenge_streak_count": streak, "challenge_streak_last_date": today}
    if last != today:
        return {"challenge_streak_count": 0, "challenge_streak_last_date": today}
    return {}


def _cas_expected(entry: dict, field: str):
    """Build an exact optimistic-CAS predicate, including legacy missing fields."""
    if field in entry:
        return entry[field]
    return {"$exists": False}


def _post_result_achievement_state(
    entry: dict,
    *,
    daily_fields: dict,
    challenge_fields: dict,
    is_perfect: bool,
    max_streak: int,
) -> dict:
    return {
        "total_tests": max(0, int(entry.get("total_tests", 0) or 0)) + 1,
        "perfect_count": max(0, int(entry.get("perfect_count", 0) or 0))
        + (1 if is_perfect else 0),
        "max_streak_ever": max(
            max(0, int(entry.get("max_streak_ever", 0) or 0)),
            max_streak,
        ),
        "daily_activity_streak": max(
            0,
            int(
                daily_fields.get(
                    "daily_activity_streak",
                    entry.get("daily_activity_streak", 0),
                )
                or 0
            ),
        ),
        "challenge_streak_count": max(
            0,
            int(
                challenge_fields.get(
                    "challenge_streak_count",
                    entry.get("challenge_streak_count", 0),
                )
                or 0
            ),
        ),
    }


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
    quiz_mode: str | None = None,
    fastest_answer: float | None = None,
) -> dict:
    """Apply counters once and persist the retry-critical result snapshot.

    Distinct results for the same user are serialized with optimistic CAS on
    ``total_tests`` plus any streak state read before the update. This avoids
    stale pre-read streak/date writes while keeping the result counters and
    receipt in one atomic user-document update.
    """
    result_id = _normalize_result_id(result_id)
    collection = _users()
    uid = database._uid(user_id)
    level_key = database._safe_level_key(level_key)
    score, total = database._validate_score(score, total)
    time_seconds = max(0.0, float(time_seconds))
    score_multiplier = max(0.0, float(score_multiplier))
    max_streak = max(0, int(max_streak))
    if fastest_answer is not None:
        fastest_answer = max(0.0, float(fastest_answer))

    # The completion clock is fixed for the whole CAS retry loop. A contention
    # retry crossing midnight must not silently move this result to a new day.
    now = database._now_utc()
    completed_at = now.isoformat()
    today = now.strftime("%Y-%m-%d")
    receipt_path = _receipt_path(result_id)

    ppq = database.POINTS_PER_QUESTION.get(level_key, 1)
    earned_base = round(score * ppq * score_multiplier)
    is_perfect = total > 0 and score == total
    durable_result = {
        "level_key": level_key,
        "score": score,
        "total": total,
        "time_seconds": time_seconds,
        "score_multiplier": score_multiplier,
        "max_streak": max_streak,
        "challenge_mode": challenge_mode,
        "quiz_mode": quiz_mode,
        "fastest_answer": fastest_answer,
        "earned_base": earned_base,
    }

    try:
        entry = _ensure_user(user_id, username, first_name)
        for _attempt in range(_BASE_CAS_RETRIES):
            daily_fields = _daily_activity_fields(entry, today)
            challenge_fields = _challenge_streak_fields(entry, today, challenge_mode, score)
            achievement_state = _post_result_achievement_state(
                entry,
                daily_fields=daily_fields,
                challenge_fields=challenge_fields,
                is_perfect=is_perfect,
                max_streak=max_streak,
            )
            receipt_snapshot = {
                "completed_at": completed_at,
                "daily_streak": achievement_state["daily_activity_streak"],
                "challenge_streak": achievement_state["challenge_streak_count"],
                "result": durable_result,
                "achievement_state": achievement_state,
            }

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
                receipt_path: receipt_snapshot,
                **daily_fields,
                **challenge_fields,
            }
            if is_perfect:
                set_fields["last_perfect_date"] = today

            query = {
                "_id": uid,
                receipt_path: {"$exists": False},
                "total_tests": _cas_expected(entry, "total_tests"),
            }
            if daily_fields:
                query["daily_activity_last"] = _cas_expected(entry, "daily_activity_last")
            if challenge_fields:
                query["challenge_streak_last_date"] = _cas_expected(
                    entry, "challenge_streak_last_date"
                )

            after = collection.find_one_and_update(
                query,
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
                    "result": durable_result,
                    "user": after,
                }

            existing = collection.find_one({"_id": uid, receipt_path: {"$exists": True}})
            if existing:
                durable_receipt = _receipt_snapshot(existing, result_id)
                if not durable_receipt.get("completed_at"):
                    raise LegacyResultStoreUnavailable("base result receipt timestamp is invalid")
                stored_result = durable_receipt.get("result") or durable_result
                return {
                    "applied": False,
                    "earned_base": int(stored_result.get("earned_base", earned_base) or 0),
                    "completed_at": durable_receipt["completed_at"],
                    "receipt": durable_receipt,
                    "result": stored_result,
                    "user": existing,
                }

            entry = collection.find_one({"_id": uid})
            if entry is None:
                raise LegacyResultStoreUnavailable("user stats document disappeared during result CAS")

        raise LegacyResultStoreUnavailable("base result CAS retry budget exhausted")
    except LegacyResultStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("base result write failed") from exc


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
            raise LegacyResultStoreUnavailable("daily bonus user document is missing")
        receipts = entry.get("daily_bonus_receipts", {})
        if isinstance(receipts, dict) and day_key in receipts:
            return _bonus_stage(receipts[day_key], claimed_now=False)

        if entry.get("last_daily_bonus", "") == day:
            receipt = {"bonus": 0, "eligible": False, "legacy": True}
            result = collection.update_one(
                {"_id": uid, receipt_path: {"$exists": False}},
                {"$set": {receipt_path: receipt}},
            )
            if result.modified_count == 1:
                return _bonus_stage(receipt, claimed_now=False)
        else:
            receipt = {"bonus": bonus, "eligible": True}
            result = collection.update_one(
                {"_id": uid, receipt_path: {"$exists": False}},
                {
                    "$set": {receipt_path: receipt},
                    "$max": {"last_daily_bonus": day},
                    "$inc": {"total_points": bonus},
                },
            )
            if result.modified_count == 1:
                return _bonus_stage(receipt, claimed_now=True)

        refreshed = collection.find_one({"_id": uid}, {receipt_path: 1}) or {}
        stored = refreshed.get("daily_bonus_receipts", {}).get(day_key)
        if stored is not None:
            return _bonus_stage(stored, claimed_now=False)
        raise LegacyResultStoreUnavailable("daily bonus receipt could not be persisted")
    except LegacyResultStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("daily bonus write failed") from exc


def claim_daily_bonus_once(user_id: int, day: str, daily_streak: int | None = None) -> int:
    """Compatibility wrapper: return points only for the winning claim."""
    if daily_streak is None:
        entry = _users().find_one(
            {"_id": database._uid(user_id)}, {"daily_activity_streak": 1}
        ) or {}
        daily_streak = int(entry.get("daily_activity_streak", 0) or 0)
    stage = claim_daily_bonus_state(user_id, day, daily_streak)
    return stage["bonus"] if stage["claimed_now"] else 0


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
            raise LegacyResultStoreUnavailable("challenge bonus user document is missing")
        receipts = entry.get("challenge_bonus_receipts", {})
        mode_receipts = receipts.get(mode, {}) if isinstance(receipts, dict) else {}
        if isinstance(mode_receipts, dict) and day_key in mode_receipts:
            return _bonus_stage(mode_receipts[day_key], claimed_now=False)

        if entry.get(date_field, "") == day:
            receipt = {"bonus": 0, "eligible": False, "legacy": True}
            result = collection.update_one(
                {"_id": uid, receipt_path: {"$exists": False}},
                {"$set": {receipt_path: receipt}},
            )
            if result.modified_count == 1:
                return _bonus_stage(receipt, claimed_now=False)
        else:
            receipt = {"bonus": bonus, "eligible": True}
            update: dict = {
                "$set": {receipt_path: receipt},
                "$max": {date_field: day},
            }
            if bonus:
                update["$inc"] = {"total_points": bonus}
            result = collection.update_one(
                {"_id": uid, receipt_path: {"$exists": False}},
                update,
            )
            if result.modified_count == 1:
                return _bonus_stage(receipt, claimed_now=True)

        refreshed = collection.find_one({"_id": uid}, {receipt_path: 1}) or {}
        stored = (
            refreshed.get("challenge_bonus_receipts", {}).get(mode, {}).get(day_key)
        )
        if stored is not None:
            return _bonus_stage(stored, claimed_now=False)
        raise LegacyResultStoreUnavailable("challenge bonus receipt could not be persisted")
    except LegacyResultStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("challenge bonus write failed") from exc


def claim_challenge_bonus_once(user_id: int, mode: str, score: int, day: str) -> int:
    """Compatibility wrapper: return points only for the winning claim."""
    stage = claim_challenge_bonus_state(user_id, mode, score, day)
    return stage["bonus"] if stage["claimed_now"] else 0


def claim_achievement_once(
    user_id: int,
    achievement_key: str,
    *,
    reward: int = 0,
    awarded_at: str | None = None,
) -> bool:
    if (
        not isinstance(achievement_key, str)
        or not achievement_key
        or "." in achievement_key
        or achievement_key.startswith("$")
    ):
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
