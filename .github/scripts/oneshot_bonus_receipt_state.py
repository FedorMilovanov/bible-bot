from pathlib import Path

path = Path("legacy_result_store.py")
text = path.read_text(encoding="utf-8")
original = text


def replace_region(start_marker: str, end_marker: str, replacement: str) -> None:
    global text
    start = text.index(start_marker)
    end = text.index(end_marker, start)
    text = text[:start] + replacement + text[end:]


def replace_once(old: str, new: str, label: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, got {count}")
    text = text.replace(old, new, 1)


replace_region(
    "def _receipt_completed_at(doc: dict, result_id: str) -> str | None:\n",
    "def _coerce_completed_at",
    '''def _receipt_snapshot(doc: dict, result_id: str) -> dict:\n    """Return the durable per-result snapshot, including legacy string receipts."""\n    receipts = doc.get("legacy_result_receipts", {})\n    if not isinstance(receipts, dict):\n        return {}\n    value = receipts.get(_receipt_digest(result_id))\n    if isinstance(value, dict):\n        completed_at = value.get("completed_at")\n        if isinstance(completed_at, datetime):\n            completed_at = completed_at.isoformat()\n        if not isinstance(completed_at, str) or not completed_at:\n            return {}\n        return {\n            "completed_at": completed_at,\n            "daily_streak": max(0, int(value.get("daily_streak", 0) or 0)),\n            "challenge_streak": max(0, int(value.get("challenge_streak", 0) or 0)),\n        }\n    if isinstance(value, datetime):\n        return {"completed_at": value.isoformat(), "daily_streak": 0, "challenge_streak": 0}\n    if isinstance(value, str) and value:\n        return {"completed_at": value, "daily_streak": 0, "challenge_streak": 0}\n    return {}\n\n\ndef _receipt_completed_at(doc: dict, result_id: str) -> str | None:\n    return _receipt_snapshot(doc, result_id).get("completed_at")\n\n\n''',
)

replace_once(
    '''    receipt_path = _receipt_path(result_id)\n    set_fields = {\n        "username": username or "",\n        "first_name": first_name or "Пользователь",\n        "last_activity": now,\n        receipt_path: completed_at,\n        **_daily_activity_fields(entry, today),\n        **_challenge_streak_fields(entry, today, challenge_mode, score),\n    }\n''',
    '''    receipt_path = _receipt_path(result_id)\n    daily_fields = _daily_activity_fields(entry, today)\n    challenge_fields = _challenge_streak_fields(entry, today, challenge_mode, score)\n    daily_streak_after = max(0, int(\n        daily_fields.get("daily_activity_streak", entry.get("daily_activity_streak", 0)) or 0\n    ))\n    challenge_streak_after = max(0, int(\n        challenge_fields.get("challenge_streak_count", entry.get("challenge_streak_count", 0)) or 0\n    ))\n    receipt_snapshot = {\n        "completed_at": completed_at,\n        "daily_streak": daily_streak_after,\n        "challenge_streak": challenge_streak_after,\n    }\n    set_fields = {\n        "username": username or "",\n        "first_name": first_name or "Пользователь",\n        "last_activity": now,\n        receipt_path: receipt_snapshot,\n        **daily_fields,\n        **challenge_fields,\n    }\n''',
    "base receipt snapshot",
)

replace_once(
    '''                "completed_at": completed_at,\n                "user": after,\n''',
    '''                "completed_at": completed_at,\n                "receipt": receipt_snapshot,\n                "user": after,\n''',
    "applied receipt return",
)

replace_once(
    '''            durable_completed_at = _receipt_completed_at(existing, result_id)\n            if durable_completed_at is None:\n                raise LegacyResultStoreUnavailable("base result receipt timestamp is invalid")\n            return {\n                "applied": False,\n                "earned_base": earned_base,\n                "completed_at": durable_completed_at,\n                "user": existing,\n            }\n''',
    '''            durable_receipt = _receipt_snapshot(existing, result_id)\n            durable_completed_at = durable_receipt.get("completed_at")\n            if durable_completed_at is None:\n                raise LegacyResultStoreUnavailable("base result receipt timestamp is invalid")\n            return {\n                "applied": False,\n                "earned_base": earned_base,\n                "completed_at": durable_completed_at,\n                "receipt": durable_receipt,\n                "user": existing,\n            }\n''',
    "duplicate receipt return",
)

replace_region(
    "def claim_daily_bonus_once(user_id: int, day: str) -> int:\n",
    "def claim_challenge_bonus_once",
    '''def _stored_bonus_amount(value) -> int:\n    if isinstance(value, bool):\n        return 0\n    try:\n        return max(0, int(value or 0))\n    except (TypeError, ValueError):\n        return 0\n\n\ndef claim_daily_bonus_state(user_id: int, day: str, daily_streak: int) -> dict:\n    """Claim or recover the normal-test bonus for the result's durable day."""\n    collection = _users()\n    uid = database._uid(user_id)\n    day_key = _day_key(day)\n    receipt_path = f"daily_bonus_receipts.{day_key}"\n    daily_streak = max(0, int(daily_streak or 0))\n    bonus = 15 if daily_streak >= 7 else 10 if daily_streak >= 3 else 5\n    try:\n        entry = collection.find_one(\n            {"_id": uid},\n            {"last_daily_bonus": 1, receipt_path: 1},\n        )\n        if not entry:\n            return {"bonus": 0, "claimed_now": False}\n        receipt_value = entry.get("daily_bonus_receipts", {}).get(day_key)\n        if receipt_value is not None:\n            return {"bonus": _stored_bonus_amount(receipt_value), "claimed_now": False}\n\n        # Migration bridge: legacy code recorded only the latest date. If that\n        # field already says this result day was paid, backfill a zero-valued\n        # receipt without awarding points again; the historical amount is unknown.\n        if entry.get("last_daily_bonus", "") == day:\n            collection.update_one(\n                {"_id": uid, receipt_path: {"$exists": False}},\n                {"$set": {receipt_path: 0}},\n            )\n            return {"bonus": 0, "claimed_now": False}\n\n        result = collection.update_one(\n            {"_id": uid, receipt_path: {"$exists": False}},\n            {\n                "$set": {receipt_path: bonus},\n                "$max": {"last_daily_bonus": day},\n                "$inc": {"total_points": bonus},\n            },\n        )\n        if result.modified_count == 1:\n            return {"bonus": bonus, "claimed_now": True}\n\n        raced = collection.find_one({"_id": uid}, {receipt_path: 1}) or {}\n        raced_value = raced.get("daily_bonus_receipts", {}).get(day_key)\n        return {"bonus": _stored_bonus_amount(raced_value), "claimed_now": False}\n    except PyMongoError as exc:\n        raise LegacyResultStoreUnavailable("daily bonus write failed") from exc\n\n\ndef claim_daily_bonus_once(user_id: int, day: str, daily_streak: int | None = None) -> int:\n    """Backward-compatible wrapper returning points only for the winning claim."""\n    if daily_streak is None:\n        entry = _users().find_one({"_id": database._uid(user_id)}, {"daily_activity_streak": 1}) or {}\n        daily_streak = int(entry.get("daily_activity_streak", 0) or 0)\n    state = claim_daily_bonus_state(user_id, day, daily_streak)\n    return state["bonus"] if state["claimed_now"] else 0\n\n\n''',
)

replace_region(
    "def claim_challenge_bonus_once(user_id: int, mode: str, score: int, day: str) -> int:\n",
    "def claim_achievement_once",
    '''def claim_challenge_bonus_state(user_id: int, mode: str, score: int, day: str) -> dict:\n    """Claim or recover a Challenge bonus for the result's durable day."""\n    if mode not in _CHALLENGE_MODES:\n        raise ValueError(f"unsupported challenge mode: {mode}")\n    collection = _users()\n    uid = database._uid(user_id)\n    day_key = _day_key(day)\n    score, _ = database._validate_score(score, 20)\n    bonus = database.compute_bonus(score, mode, True)\n    date_field = f"{mode}_last_bonus_date"\n    receipt_path = f"challenge_bonus_receipts.{mode}.{day_key}"\n    try:\n        entry = collection.find_one(\n            {"_id": uid},\n            {date_field: 1, receipt_path: 1},\n        )\n        if not entry:\n            return {"bonus": 0, "claimed_now": False}\n        receipt_value = (\n            entry.get("challenge_bonus_receipts", {})\n            .get(mode, {})\n            .get(day_key)\n        )\n        if receipt_value is not None:\n            return {"bonus": _stored_bonus_amount(receipt_value), "claimed_now": False}\n\n        # Migration bridge for pre-receipt deployments.\n        if entry.get(date_field, "") == day:\n            collection.update_one(\n                {"_id": uid, receipt_path: {"$exists": False}},\n                {"$set": {receipt_path: 0}},\n            )\n            return {"bonus": 0, "claimed_now": False}\n\n        update: dict = {\n            "$set": {receipt_path: bonus},\n            "$max": {date_field: day},\n        }\n        if bonus:\n            update["$inc"] = {"total_points": bonus}\n        result = collection.update_one(\n            {"_id": uid, receipt_path: {"$exists": False}},\n            update,\n        )\n        if result.modified_count == 1:\n            return {"bonus": bonus, "claimed_now": True}\n\n        raced = collection.find_one({"_id": uid}, {receipt_path: 1}) or {}\n        raced_value = (\n            raced.get("challenge_bonus_receipts", {})\n            .get(mode, {})\n            .get(day_key)\n        )\n        return {"bonus": _stored_bonus_amount(raced_value), "claimed_now": False}\n    except PyMongoError as exc:\n        raise LegacyResultStoreUnavailable("challenge bonus write failed") from exc\n\n\ndef claim_challenge_bonus_once(user_id: int, mode: str, score: int, day: str) -> int:\n    """Backward-compatible wrapper returning points only for the winning claim."""\n    state = claim_challenge_bonus_state(user_id, mode, score, day)\n    return state["bonus"] if state["claimed_now"] else 0\n\n\n''',
)

if text == original:
    raise SystemExit("no changes produced")

required = [
    '"receipt": receipt_snapshot',
    '"daily_streak": daily_streak_after',
    'def claim_daily_bonus_state(',
    '"$set": {receipt_path: bonus}',
    'def claim_challenge_bonus_state(',
]
for marker in required:
    if marker not in text:
        raise SystemExit(f"required marker missing: {marker}")

path.write_text(text, encoding="utf-8")
