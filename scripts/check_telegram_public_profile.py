#!/usr/bin/env python3
"""Read-only check for the canonical public Telegram bot profile.

Exit codes:
  0 - default and Russian public profile values match the canonical contract
  1 - Telegram is reachable but one or more public profile fields differ
  2 - provider state could not be established

The script calls only getMyName/getMyShortDescription/getMyDescription. It never
mutates Telegram provider state and never prints BOT_TOKEN or request URLs.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from telegram_public_profile import (  # noqa: E402
    PUBLIC_BOT_DESCRIPTION,
    PUBLIC_BOT_NAME,
    PUBLIC_BOT_SHORT_DESCRIPTION,
    PUBLIC_PROFILE_LANGUAGE_CODES,
)

SAFE = 0
UNSAFE = 1
UNAVAILABLE = 2

_FIELDS = (
    ("name", "getMyName", "name", PUBLIC_BOT_NAME),
    (
        "short_description",
        "getMyShortDescription",
        "short_description",
        PUBLIC_BOT_SHORT_DESCRIPTION,
    ),
    ("description", "getMyDescription", "description", PUBLIC_BOT_DESCRIPTION),
)


def _fetch_value(
    bot_token: str,
    method: str,
    result_key: str,
    language_code: str,
    *,
    timeout: float = 5.0,
) -> str:
    query = urlencode({"language_code": language_code}) if language_code else ""
    suffix = f"?{query}" if query else ""
    request = Request(
        f"https://api.telegram.org/bot{bot_token}/{method}{suffix}",
        headers={"Accept": "application/json"},
        method="GET",
    )
    with urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise RuntimeError(f"Telegram did not return a successful {method} response")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"Telegram {method} result is malformed")
    value = result.get(result_key)
    if not isinstance(value, str):
        raise RuntimeError(f"Telegram {method} field is malformed")
    return value


def _profile_mismatches(bot_token: str) -> list[str]:
    mismatches: list[str] = []
    for language_code in PUBLIC_PROFILE_LANGUAGE_CODES:
        locale = language_code or "default"
        for field, method, result_key, expected in _FIELDS:
            actual = _fetch_value(bot_token, method, result_key, language_code)
            if actual != expected:
                mismatches.append(f"{locale}:{field}")
    return mismatches


def main() -> int:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        print(json.dumps({"error": "BOT_TOKEN is required"}, sort_keys=True))
        return UNAVAILABLE

    try:
        mismatches = _profile_mismatches(token)
    except Exception:
        print(json.dumps({"error": "telegram public profile check failed"}, sort_keys=True))
        return UNAVAILABLE

    payload = {
        "profile_ok": not mismatches,
        "mismatches": mismatches,
        "locales": ["default", "ru"],
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return SAFE if not mismatches else UNSAFE


if __name__ == "__main__":
    raise SystemExit(main())
