"""Telegram Mini App authentication helpers."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from urllib.parse import parse_qsl

from flask import jsonify, request

DEFAULT_INIT_DATA_MAX_AGE = 24 * 60 * 60


def verify_init_data(init_data: str) -> dict | None:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token or not init_data:
        return None

    try:
        data = dict(parse_qsl(init_data, keep_blank_values=True, strict_parsing=True))
        received_hash = data.pop("hash", None)
        if not received_hash:
            return None

        check_string = "\n".join(f"{key}={value}" for key, value in sorted(data.items()))
        secret_key = hmac.new(b"WebAppData", token.encode("utf-8"), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, check_string.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calculated_hash, received_hash):
            return None

        auth_date = int(data.get("auth_date", "0"))
        now = int(time.time())
        max_age = int(os.getenv("TELEGRAM_INIT_DATA_MAX_AGE_SECONDS", DEFAULT_INIT_DATA_MAX_AGE))
        if auth_date <= 0 or auth_date > now + 60 or now - auth_date > max_age:
            return None

        user = json.loads(data.get("user", ""))
        if not isinstance(user, dict) or not user.get("id"):
            return None
        return user
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def get_user_from_request() -> dict | None:
    user = verify_init_data(request.headers.get("X-Telegram-Init-Data", ""))
    if user:
        return user

    # Explicit development-only escape hatch. Query-string user IDs are never accepted.
    if os.getenv("APP_ENV", "production").lower() == "development" and os.getenv(
        "ALLOW_DEBUG_AUTH", "false"
    ).lower() in {"1", "true", "yes"}:
        debug_uid = request.headers.get("X-Debug-User-Id", "")
        if debug_uid.isdigit():
            return {"id": int(debug_uid), "first_name": "Local Debug User"}
    return None


def require_user():
    user = get_user_from_request()
    if user:
        return user, None
    return None, (jsonify({"error": "telegram authentication required"}), 401)
