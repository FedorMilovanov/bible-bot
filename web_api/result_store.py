"""Crash-safe, idempotent persistence for Mini App aggregate results.

A receipt is written into the same MongoDB user document and in the same atomic
update as points/attempts. If the process dies before the quiz session is marked
finished, a retry sees the receipt and returns the already-applied result.
Receipts are retained longer than the Mini App session TTL, then pruned lazily
on later result writes so the user document cannot grow without bound.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
_RESULT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_RECEIPT_RETENTION = timedelta(hours=24)


def _today_utc() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _receipt_field(result_id: str) -> str:
    if not _RESULT_ID_RE.fullmatch(result_id or ""):
        raise ValueError("invalid Mini App result id")
    return f"miniapp_result_receipts.{result_id}"


def _receipt_from(entry: dict | None, result_id: str) -> dict | None:
    receipts = (entry or {}).get("miniapp_result_receipts") or {}
    receipt = receipts.get(result_id) if isinstance(receipts, dict) else None
    return dict(receipt) if isinstance(receipt, dict) else None


def _user_collection():
    import database

    return database.collection


def _prune_old_receipts(user_id: int) -> None:
    """Best-effort cleanup; only receipts much older than the 6h session TTL qualify."""
    collection = _user_collection()
    if collection is None:
        return

    try:
        entry = collection.find_one({"_id": str(user_id)}) or {}
        receipts = entry.get("miniapp_result_receipts") or {}
        if not isinstance(receipts, dict):
            return
        cutoff = datetime.utcnow() - _RECEIPT_RETENTION
        stale = []
        for result_id, receipt in receipts.items():
            applied_at = receipt.get("applied_at") if isinstance(receipt, dict) else None
            if isinstance(applied_at, datetime) and applied_at < cutoff:
                stale.append(result_id)
        if stale:
            collection.update_one(
                {"_id": str(user_id)},
                {"$unset": {_receipt_field(result_id): "" for result_id in stale}},
            )
    except Exception:
        logger.warning("could not prune old Mini App result receipts", exc_info=True)


def _persist_once(user_id: int, result_id: str, update: dict, receipt: dict) -> dict | None:
    collection = _user_collection()
    if collection is None:
        return None

    uid = str(user_id)
    field = _receipt_field(result_id)
    stored_receipt = dict(receipt)
    stored_receipt["applied_at"] = datetime.utcnow()
    update.setdefault("$set", {})[field] = stored_receipt
    try:
        result = collection.update_one(
            {"_id": uid, field: {"$exists": False}},
            update,
            upsert=False,
        )
        if getattr(result, "modified_count", 0) == 1:
            _prune_old_receipts(user_id)
            return dict(stored_receipt)

        existing = _receipt_from(collection.find_one({"_id": uid}), result_id)
        if existing is not None:
            _prune_old_receipts(user_id)
        return existing
    except Exception:
        logger.exception("failed to persist Mini App result receipt %s", result_id)
        return None


def apply_regular_result_once(
    *,
    user_id: int,
    result_id: str,
    username: str,
    first_name: str,
    level_key: str,
    score: int,
    total: int,
    time_seconds: float,
    score_multiplier: float,
    is_perfect: bool,
    max_streak: int,
) -> dict | None:
    """Atomically apply a normal Mini App result and its daily/achievement stats."""
    import database

    collection = database.collection
    if collection is None or level_key not in database.ALL_LEVEL_KEYS:
        return None

    uid = str(user_id)
    existing = collection.find_one({"_id": uid})
    if not existing:
        return None
    prior_receipt = _receipt_from(existing, result_id)
    if prior_receipt:
        return prior_receipt

    score = max(0, min(int(score), int(total)))
    total = max(1, int(total))
    multiplier = max(0.0, float(score_multiplier))
    today = _today_utc()

    last_activity = existing.get("daily_activity_last", "")
    daily_streak = int(existing.get("daily_activity_streak", 0))
    if last_activity != today:
        if last_activity:
            try:
                delta = (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(last_activity, "%Y-%m-%d")).days
                daily_streak = daily_streak + 1 if delta == 1 else 1
            except Exception:
                daily_streak = 1
        else:
            daily_streak = 1

    bonus = 0
    if existing.get("last_daily_bonus", "") != today:
        bonus = 15 if daily_streak >= 7 else 10 if daily_streak >= 3 else 5

    ppq = database.POINTS_PER_QUESTION.get(level_key, 1)
    base_points = round(score * ppq * multiplier)
    awarded_points = base_points + bonus
    receipt = {
        "points": awarded_points,
        "daily_bonus": bonus,
        "new_achievements": [],
        "kind": "regular",
        "level_key": level_key,
    }

    set_fields = {
        "username": username or "",
        "first_name": first_name or "Пользователь",
        "last_activity": datetime.utcnow(),
    }
    if last_activity != today:
        set_fields["daily_activity_streak"] = daily_streak
        set_fields["daily_activity_last"] = today
    if bonus:
        set_fields["last_daily_bonus"] = today
    if is_perfect:
        set_fields["last_perfect_date"] = today

    inc_fields = {
        "total_tests": 1,
        "total_questions_answered": total,
        "total_correct_answers": score,
        "total_time_spent": max(0.0, float(time_seconds)),
        "total_points": awarded_points,
        f"{level_key}_attempts": 1,
        f"{level_key}_correct": score,
        f"{level_key}_total": total,
    }
    if is_perfect:
        inc_fields["perfect_count"] = 1

    update = {
        "$inc": inc_fields,
        "$set": set_fields,
        "$max": {
            f"{level_key}_best_score": score,
            "max_streak_ever": max(0, int(max_streak)),
        },
    }
    return _persist_once(user_id, result_id, update, receipt)


def apply_challenge_result_once(
    *,
    user_id: int,
    result_id: str,
    username: str,
    first_name: str,
    mode: str,
    score: int,
    total: int,
    time_seconds: float,
) -> dict | None:
    """Atomically apply Challenge 20 aggregates, bonus, streak and achievements."""
    import database

    collection = database.collection
    if collection is None or mode not in {"random20", "hardcore20"}:
        return None

    uid = str(user_id)
    existing = collection.find_one({"_id": uid})
    if not existing:
        return None
    prior_receipt = _receipt_from(existing, result_id)
    if prior_receipt:
        return prior_receipt

    score = max(0, min(int(score), int(total)))
    total = max(1, int(total))
    today = _today_utc()
    eligible = existing.get(f"{mode}_last_bonus_date", "") != today
    bonus = database.compute_bonus(score, mode, eligible)
    base_points = score * database.POINTS_PER_QUESTION.get(mode, 1)
    awarded_points = base_points + bonus

    achievements = dict(existing.get("achievements") or {})
    new_achievements: list[str] = []
    set_fields = {
        "username": username or "",
        "first_name": first_name or "Пользователь",
        "last_activity": datetime.utcnow(),
    }
    if eligible:
        set_fields[f"{mode}_last_bonus_date"] = today

    streak_last = existing.get("challenge_streak_last_date", "")
    if score >= 18:
        streak_count = int(existing.get("challenge_streak_count", 0))
        if not streak_last:
            streak_count = 1
        else:
            try:
                delta = (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(streak_last, "%Y-%m-%d")).days
                if delta == 1:
                    streak_count += 1
                elif delta != 0:
                    streak_count = 1
            except Exception:
                streak_count = 1
        set_fields["challenge_streak_count"] = streak_count
        set_fields["challenge_streak_last_date"] = today
        if streak_count >= 3 and "streak_3" not in achievements:
            achievements["streak_3"] = today
            new_achievements.append("🔥 3-дневная серия 18+ — разблокировано!")
    elif streak_last != today:
        set_fields["challenge_streak_count"] = 0
        set_fields["challenge_streak_last_date"] = today

    if score == 20 and "perfect_20" not in achievements:
        achievements["perfect_20"] = today
        new_achievements.append("⭐ Perfect 20 — разблокировано!")
    if new_achievements:
        set_fields["achievements"] = achievements

    receipt = {
        "points": awarded_points,
        "daily_bonus": bonus,
        "new_achievements": new_achievements,
        "kind": "challenge",
        "level_key": mode,
    }
    update = {
        "$inc": {
            "total_tests": 1,
            "total_questions_answered": total,
            "total_correct_answers": score,
            "total_time_spent": max(0.0, float(time_seconds)),
            "total_points": awarded_points,
            f"{mode}_attempts": 1,
            f"{mode}_correct": score,
            f"{mode}_total": total,
        },
        "$set": set_fields,
        "$max": {f"{mode}_best_score": score},
    }
    return _persist_once(user_id, result_id, update, receipt)
