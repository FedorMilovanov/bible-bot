"""Result-owned bonus receipts for crash-safe legacy quiz finalization.

Daily and Challenge bonuses are limited per calendar day, but recovery happens
per quiz result. ``legacy_result_store`` atomically reserves the first durable
normal/day or Challenge-mode/day result owner together with base scoring. This
module only pays or recovers a bonus for that reserved owner.
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


def _entry_field(entry: dict, path: str) -> tuple[object | None, bool]:
    current: object = entry
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current[part]
    return current, True


def _normal_owner_path(day_key: str) -> str:
    return f"normal_bonus_result_owners.{day_key}"


def _challenge_owner_path(mode: str, day_key: str) -> str:
    return f"challenge_bonus_result_owners.{mode}.{day_key}"


def _stage(receipt, *, owner: str, claimed_now: bool) -> dict:
    if not isinstance(receipt, dict):
        raise LegacyResultStoreUnavailable("bonus receipt has invalid shape")
    stored_owner = receipt.get("result_owner")
    if stored_owner != owner:
        raise LegacyResultStoreUnavailable(
            "bonus receipt owner contradicts the durable first-result owner"
        )
    try:
        bonus = max(0, int(receipt.get("bonus", 0) or 0))
    except (TypeError, ValueError) as exc:
        raise LegacyResultStoreUnavailable("bonus receipt amount is invalid") from exc
    eligible = receipt.get("eligible", False)
    if not isinstance(eligible, bool):
        raise LegacyResultStoreUnavailable("bonus receipt eligibility is invalid")
    return {
        "bonus": bonus,
        "eligible": eligible,
        "claimed_now": claimed_now,
    }


def _backfill_legacy_receipt(collection, *, uid: str, receipt_path: str) -> dict:
    receipt = {"bonus": 0, "eligible": False, "legacy": True}
    result = collection.update_one(
        {"_id": uid, receipt_path: {"$exists": False}},
        {"$set": {receipt_path: receipt}},
    )
    if result.modified_count == 1:
        return {"bonus": 0, "eligible": False, "claimed_now": False}
    refreshed = collection.find_one({"_id": uid}, {receipt_path: 1}) or {}
    current: object = refreshed
    for part in receipt_path.split("."):
        if not isinstance(current, dict) or part not in current:
            current = None
            break
        current = current[part]
    if isinstance(current, dict) and current.get("legacy") is True:
        return {"bonus": 0, "eligible": False, "claimed_now": False}
    raise LegacyResultStoreUnavailable("legacy bonus receipt could not be backfilled")


def claim_daily_bonus_for_result(
    *,
    user_id: int,
    result_id: str,
    day: str,
    daily_streak: int,
) -> dict:
    """Claim/recover the normal bonus only for the first durable normal result."""
    collection = _users()
    uid = database._uid(user_id)
    owner = _owner(result_id)
    day_key = _day_key(day)
    receipt_path = f"daily_bonus_receipts.{day_key}"
    owner_path = _normal_owner_path(day_key)
    daily_streak = max(0, int(daily_streak or 0))
    bonus = 15 if daily_streak >= 7 else 10 if daily_streak >= 3 else 5

    try:
        entry = collection.find_one(
            {"_id": uid},
            {"last_daily_bonus": 1, receipt_path: 1, owner_path: 1},
        )
        if not entry:
            raise LegacyResultStoreUnavailable("daily bonus user document is missing")

        receipts = entry.get("daily_bonus_receipts", {})
        stored_receipt = receipts.get(day_key) if isinstance(receipts, dict) else None
        owner_marker, owner_exists = _entry_field(entry, owner_path)

        if isinstance(stored_receipt, dict) and stored_receipt.get("legacy") is True:
            return {"bonus": 0, "eligible": False, "claimed_now": False}

        if entry.get("last_daily_bonus", "") == day and stored_receipt is None:
            # Pre-owner/pre-receipt deployment already credited this day. Do
            # not assign the historical credit to the newly reserved result.
            return _backfill_legacy_receipt(
                collection,
                uid=uid,
                receipt_path=receipt_path,
            )

        if not owner_exists:
            raise LegacyResultStoreUnavailable(
                "daily bonus first-result owner marker is missing"
            )
        if owner_marker != owner:
            return {"bonus": 0, "eligible": False, "claimed_now": False}

        if stored_receipt is not None:
            return _stage(stored_receipt, owner=owner, claimed_now=False)

        receipt = {
            "bonus": bonus,
            "eligible": True,
            "result_owner": owner,
        }
        result = collection.update_one(
            {
                "_id": uid,
                receipt_path: {"$exists": False},
                owner_path: owner,
            },
            {
                "$set": {receipt_path: receipt},
                "$max": {"last_daily_bonus": day},
                "$inc": {"total_points": bonus},
            },
        )
        if result.modified_count == 1:
            return _stage(receipt, owner=owner, claimed_now=True)

        refreshed = collection.find_one(
            {"_id": uid},
            {receipt_path: 1, owner_path: 1},
        ) or {}
        refreshed_owner, refreshed_owner_exists = _entry_field(refreshed, owner_path)
        if not refreshed_owner_exists or refreshed_owner != owner:
            raise LegacyResultStoreUnavailable(
                "daily bonus owner changed or disappeared during claim"
            )
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
    """Claim/recover a Challenge bonus only for the first durable mode/day result."""
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
    owner_path = _challenge_owner_path(mode, day_key)

    try:
        entry = collection.find_one(
            {"_id": uid},
            {date_field: 1, receipt_path: 1, owner_path: 1},
        )
        if not entry:
            raise LegacyResultStoreUnavailable("challenge bonus user document is missing")

        receipts = entry.get("challenge_bonus_receipts", {})
        mode_receipts = receipts.get(mode, {}) if isinstance(receipts, dict) else {}
        stored_receipt = (
            mode_receipts.get(day_key) if isinstance(mode_receipts, dict) else None
        )
        owner_marker, owner_exists = _entry_field(entry, owner_path)

        if isinstance(stored_receipt, dict) and stored_receipt.get("legacy") is True:
            return {"bonus": 0, "eligible": False, "claimed_now": False}

        if entry.get(date_field, "") == day and stored_receipt is None:
            return _backfill_legacy_receipt(
                collection,
                uid=uid,
                receipt_path=receipt_path,
            )

        if not owner_exists:
            raise LegacyResultStoreUnavailable(
                "challenge bonus first-result owner marker is missing"
            )
        if owner_marker != owner:
            return {"bonus": 0, "eligible": False, "claimed_now": False}

        if stored_receipt is not None:
            return _stage(stored_receipt, owner=owner, claimed_now=False)

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
            {
                "_id": uid,
                receipt_path: {"$exists": False},
                owner_path: owner,
            },
            update,
        )
        if result.modified_count == 1:
            return _stage(receipt, owner=owner, claimed_now=True)

        refreshed = collection.find_one(
            {"_id": uid},
            {receipt_path: 1, owner_path: 1},
        ) or {}
        refreshed_owner, refreshed_owner_exists = _entry_field(refreshed, owner_path)
        if not refreshed_owner_exists or refreshed_owner != owner:
            raise LegacyResultStoreUnavailable(
                "challenge bonus owner changed or disappeared during claim"
            )
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
