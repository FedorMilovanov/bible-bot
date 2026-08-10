"""Result-owned bonus receipts for crash-safe legacy quiz finalization.

Daily and Challenge bonuses are limited per calendar day, but recovery happens
per quiz result. A day-only receipt is ambiguous: a retry of result B could see
the bonus won by result A. This module binds each daily receipt to the stable
result id that actually claimed it.
"""
from __future__ import annotations

import hashlib
from datetime import datetime

from pymongo.errors import PyMongoError

import database
from legacy_result_store import LegacyResultStoreUnavailable

_CHALLENGE_MODES = frozenset({"random20", "hardcore20"})


def _users():
    collection = getattr(database, "collection", None)
    if collection is None:
        raise LegacyResultStoreUnavailable("user stats collection is unavailable")
    return collection


def _owner(result_id: str) -> str:
    value = str(result_id or "").strip()
    if not value:
        raise ValueError("result_id is required for result-owned bonus recovery")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _day_key(day: str) -> str:
    try:
        parsed = datetime.strptime(day, "%Y-%m-%d")
    except (TypeError, ValueError) as exc:
        raise ValueError("result day must be YYYY-MM-DD") from exc
    return parsed.strftime("%Y%m%d")


def _stage(receipt, *, owner: str, claimed_now: bool) -> dict:
    if not isinstance(receipt, dict):
        return {"bonus": 0, "eligible": False, "claimed_now": False}
    stored_owner = receipt.get("result_owner")
    if stored_owner != owner:
        return {"bonus": 0, "eligible": False, "claimed_now": False}
    return {
        "bonus": max(0, int(receipt.get("bonus", 0) or 0)),
        "eligible": bool(receipt.get("eligible", False)),
        "claimed_now": claimed_now,
    }


def claim_daily_bonus_for_result(
    *,
    user_id: int,
    result_id: str,
    day: str,
    daily_streak: int,
) -> dict:
    """Claim/recover the once-per-day normal-test bonus for one result."""
    collection = _users()
    uid = database._uid(user_id)
    owner = _owner(result_id)
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
            return _stage(receipts[day_key], owner=owner, claimed_now=False)

        if entry.get("last_daily_bonus", "") == day:
            # Pre-receipt deployment already credited this day. Backfill a
            # conservative marker without assigning that legacy credit to the
            # current result.
            receipt = {"bonus": 0, "eligible": False, "legacy": True}
            result = collection.update_one(
                {"_id": uid, receipt_path: {"$exists": False}},
                {"$set": {receipt_path: receipt}},
            )
            if result.modified_count == 1:
                return {"bonus": 0, "eligible": False, "claimed_now": False}
        else:
            receipt = {
                "bonus": bonus,
                "eligible": True,
                "result_owner": owner,
            }
            result = collection.update_one(
                {"_id": uid, receipt_path: {"$exists": False}},
                {
                    "$set": {receipt_path: receipt},
                    "$max": {"last_daily_bonus": day},
                    "$inc": {"total_points": bonus},
                },
            )
            if result.modified_count == 1:
                return _stage(receipt, owner=owner, claimed_now=True)

        refreshed = collection.find_one({"_id": uid}, {receipt_path: 1}) or {}
        stored = refreshed.get("daily_bonus_receipts", {}).get(day_key)
        if stored is not None:
            return _stage(stored, owner=owner, claimed_now=False)
        raise LegacyResultStoreUnavailable("daily bonus receipt could not be persisted")
    except LegacyResultStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("daily bonus write failed") from exc


def claim_challenge_bonus_for_result(
    *,
    user_id: int,
    result_id: str,
    mode: str,
    score: int,
    day: str,
) -> dict:
    """Claim/recover one Challenge day's bonus for the result that won it."""
    if mode not in _CHALLENGE_MODES:
        raise ValueError(f"unsupported challenge mode: {mode}")

    collection = _users()
    uid = database._uid(user_id)
    owner = _owner(result_id)
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
            return _stage(mode_receipts[day_key], owner=owner, claimed_now=False)

        if entry.get(date_field, "") == day:
            receipt = {"bonus": 0, "eligible": False, "legacy": True}
            result = collection.update_one(
                {"_id": uid, receipt_path: {"$exists": False}},
                {"$set": {receipt_path: receipt}},
            )
            if result.modified_count == 1:
                return {"bonus": 0, "eligible": False, "claimed_now": False}
        else:
            receipt = {
                "bonus": bonus,
                "eligible": True,
                "result_owner": owner,
            }
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
                return _stage(receipt, owner=owner, claimed_now=True)

        refreshed = collection.find_one({"_id": uid}, {receipt_path: 1}) or {}
        stored = (
            refreshed.get("challenge_bonus_receipts", {}).get(mode, {}).get(day_key)
        )
        if stored is not None:
            return _stage(stored, owner=owner, claimed_now=False)
        raise LegacyResultStoreUnavailable("challenge bonus receipt could not be persisted")
    except LegacyResultStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("challenge bonus write failed") from exc
