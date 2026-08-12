#!/usr/bin/env python3
"""Read-only Telegram webhook deployment preflight.

Exit codes:
  0 - webhook contract is safe
  1 - Telegram is reachable but the deployed webhook contract is unsafe
  2 - the contract could not be established

The script only calls getWebhookInfo. It never registers, deletes or modifies a
webhook and never prints BOT_TOKEN.
"""
from __future__ import annotations

import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

SAFE = 0
UNSAFE = 1
UNAVAILABLE = 2
WEBHOOK_PATH = "/telegram/webhook"
EXPECTED_MAX_CONNECTIONS = 1
EXPECTED_ALLOWED_UPDATES = frozenset({"message", "callback_query"})
DEFAULT_ERROR_MAX_AGE_SECONDS = 300


def _expected_webhook_url() -> str:
    raw = (
        os.getenv("TELEGRAM_WEBHOOK_BASE_URL", "").strip()
        or os.getenv("RENDER_EXTERNAL_URL", "").strip()
    )
    if not raw:
        raise ValueError(
            "TELEGRAM_WEBHOOK_BASE_URL or RENDER_EXTERNAL_URL is required"
        )
    parsed = urlsplit(raw)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("webhook base URL must be an HTTPS origin")
    return f"{raw.rstrip('/')}{WEBHOOK_PATH}"


def _fetch_info(bot_token: str, *, timeout: float = 10.0) -> dict:
    # Keep the token only inside the request URL. Never include this URL in logs
    # or exceptions surfaced to the operator.
    request = Request(
        f"https://api.telegram.org/bot{bot_token}/getWebhookInfo",
        headers={"Accept": "application/json"},
        method="GET",
    )
    with urlopen(request, timeout=timeout) as response:  # noqa: S310 - fixed HTTPS host
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise RuntimeError("Telegram did not return a successful getWebhookInfo response")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("Telegram getWebhookInfo result is malformed")
    return result


def _classify(
    info: dict,
    *,
    expected_url: str,
    now: int | None = None,
    error_max_age_seconds: int = DEFAULT_ERROR_MAX_AGE_SECONDS,
) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    if info.get("url") != expected_url:
        errors.append("webhook URL does not match the expected deployment origin")

    max_connections = info.get("max_connections")
    if max_connections != EXPECTED_MAX_CONNECTIONS:
        errors.append(
            f"max_connections must be {EXPECTED_MAX_CONNECTIONS}, got {max_connections!r}"
        )

    allowed = info.get("allowed_updates")
    if not isinstance(allowed, list) or set(allowed) != EXPECTED_ALLOWED_UPDATES:
        errors.append(
            "allowed_updates must be exactly message + callback_query"
        )

    pending = info.get("pending_update_count", 0)
    if isinstance(pending, bool) or not isinstance(pending, int) or pending < 0:
        errors.append("pending_update_count is malformed")
    elif pending:
        warnings.append(f"Telegram currently reports {pending} pending update(s)")

    last_error_date = info.get("last_error_date")
    if last_error_date is not None:
        if (
            isinstance(last_error_date, bool)
            or not isinstance(last_error_date, int)
            or last_error_date < 0
        ):
            errors.append("last_error_date is malformed")
        else:
            current = int(time.time()) if now is None else int(now)
            age = max(0, current - last_error_date)
            message = str(info.get("last_error_message") or "Telegram delivery error")
            if age <= max(0, int(error_max_age_seconds)):
                errors.append(
                    f"Telegram reports a recent delivery error ({age}s ago): {message}"
                )
            else:
                warnings.append(
                    f"Telegram retains an older delivery error ({age}s ago): {message}"
                )

    return errors, warnings


def main() -> int:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        print("UNAVAILABLE: BOT_TOKEN is not present in this authorized environment")
        return UNAVAILABLE

    try:
        expected_url = _expected_webhook_url()
        info = _fetch_info(token)
    except (ValueError, HTTPError, URLError, TimeoutError, OSError, RuntimeError, json.JSONDecodeError):
        # Deliberately do not echo exception text: urllib errors can contain the
        # request URL, which contains BOT_TOKEN.
        print("UNAVAILABLE: could not establish Telegram webhook state")
        return UNAVAILABLE

    raw_age = os.getenv(
        "TELEGRAM_WEBHOOK_ERROR_MAX_AGE_SECONDS",
        str(DEFAULT_ERROR_MAX_AGE_SECONDS),
    ).strip()
    try:
        error_max_age = max(0, int(raw_age))
    except ValueError:
        print("UNAVAILABLE: TELEGRAM_WEBHOOK_ERROR_MAX_AGE_SECONDS is invalid")
        return UNAVAILABLE

    errors, warnings = _classify(
        info,
        expected_url=expected_url,
        error_max_age_seconds=error_max_age,
    )
    for warning in warnings:
        print(f"WARNING: {warning}")
    if errors:
        for error in errors:
            print(f"UNSAFE: {error}")
        return UNSAFE

    pending = int(info.get("pending_update_count", 0) or 0)
    print(
        "SAFE: Telegram webhook URL, allowed updates and single-connection "
        f"contract are exact; pending_update_count={pending}"
    )
    return SAFE


if __name__ == "__main__":
    sys.exit(main())
