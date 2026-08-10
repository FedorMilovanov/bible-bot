from pathlib import Path

path = Path("legacy_result_store.py")
text = path.read_text(encoding="utf-8")

start_marker = "def _stored_bonus_amount"
end_marker = "def claim_achievement_once"
if text.count(start_marker) != 1 or text.count(end_marker) != 1:
    raise SystemExit("bonus region markers are not unique")

start = text.index(start_marker)
end = text.index(end_marker, start)
replacement = '''def _bonus_stage(receipt, *, claimed_now: bool) -> dict:
    if isinstance(receipt, dict):
        bonus = max(0, int(receipt.get("bonus", 0) or 0))
        eligible = bool(receipt.get("eligible", False))
    elif isinstance(receipt, bool):
        bonus = 0
        eligible = False
    else:
        try:
            bonus = max(0, int(receipt or 0))
        except (TypeError, ValueError):
            bonus = 0
        eligible = True
    return {"bonus": bonus, "eligible": eligible, "claimed_now": bool(claimed_now)}


def claim_daily_bonus_once(
    user_id: int,
    day: str,
    daily_streak: int | None = None,
) -> dict:
    """Atomically award/replay the normal-test daily bonus for a durable day."""
    collection = _users()
    uid = database._uid(user_id)
    day_key = _day_key(day)
    receipt_path = f"daily_bonus_receipts.{day_key}"
    try:
        entry = collection.find_one(
            {"_id": uid},
            {"daily_activity_streak": 1, "last_daily_bonus": 1, receipt_path: 1},
        )
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
            refreshed = collection.find_one({"_id": uid}, {receipt_path: 1}) or {}
            stored = refreshed.get("daily_bonus_receipts", {}).get(day_key)
            if stored is not None:
                return _bonus_stage(stored, claimed_now=False)
            raise LegacyResultStoreUnavailable("daily bonus migration receipt disappeared")

        streak = (
            max(0, int(daily_streak or 0))
            if daily_streak is not None
            else max(0, int(entry.get("daily_activity_streak", 0) or 0))
        )
        bonus = 15 if streak >= 7 else 10 if streak >= 3 else 5
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


def claim_challenge_bonus_once(user_id: int, mode: str, score: int, day: str) -> dict:
    """Atomically award/replay a Challenge bonus for a durable result day."""
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
            refreshed = collection.find_one({"_id": uid}, {receipt_path: 1}) or {}
            stored = refreshed.get("challenge_bonus_receipts", {}).get(mode, {}).get(day_key)
            if stored is not None:
                return _bonus_stage(stored, claimed_now=False)
            raise LegacyResultStoreUnavailable("challenge bonus migration receipt disappeared")

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
        stored = refreshed.get("challenge_bonus_receipts", {}).get(mode, {}).get(day_key)
        if stored is not None:
            return _bonus_stage(stored, claimed_now=False)
        raise LegacyResultStoreUnavailable("challenge bonus receipt could not be persisted")
    except LegacyResultStoreUnavailable:
        raise
    except PyMongoError as exc:
        raise LegacyResultStoreUnavailable("challenge bonus write failed") from exc


'''

updated = text[:start] + replacement + text[end:]
for marker in (
    "def _bonus_stage(",
    '"eligible": True',
    '"legacy": True',
    'daily_streak: int | None = None',
):
    if marker not in updated:
        raise SystemExit(f"required marker missing: {marker}")
if updated == text:
    raise SystemExit("no changes produced")
path.write_text(updated, encoding="utf-8")
