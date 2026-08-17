#!/usr/bin/env python3
"""Read-only check for BotFather Main Mini App provider state.

Exit codes:
  0 - Main Mini App is enabled
  1 - Telegram is reachable but Main Mini App is not enabled
  2 - provider state could not be established

The script only calls getMe. It never mutates bot/provider state and never
prints BOT_TOKEN.
"""
from __future__ import annotations

import json
import os
from urllib.request import Request, urlopen

ENABLED = 0
DISABLED = 1
UNAVAILABLE = 2


def _fetch_status(bot_token: str, *, timeout: float = 10.0) -> dict[str, object]:
    # Keep the token only inside the request URL. Never include this URL in logs
    # or exceptions surfaced to the operator.
    request = Request(
        f"https://api.telegram.org/bot{bot_token}/getMe",
        headers={"Accept": "application/json"},
        method="GET",
    )
    with urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise RuntimeError("Telegram did not return a successful getMe response")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("Telegram getMe result is malformed")
    return {
        "username": str(result.get("username") or "").strip().lstrip("@"),
        "has_main_web_app": bool(result.get("has_main_web_app", False)),
    }


def main() -> int:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        print(json.dumps({"error": "BOT_TOKEN is required"}, sort_keys=True))
        return UNAVAILABLE

    try:
        status = _fetch_status(token)
    except Exception:
        print(json.dumps({"error": "telegram provider check failed"}, sort_keys=True))
        return UNAVAILABLE

    print(json.dumps(status, ensure_ascii=False, sort_keys=True))
    return ENABLED if status["has_main_web_app"] else DISABLED


if __name__ == "__main__":
    raise SystemExit(main())
