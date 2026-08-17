#!/usr/bin/env python3
"""Check whether BotFather Main Mini App is enabled for the production bot."""
from __future__ import annotations

import asyncio
import json
import os

from telegram import Bot

from telegram_provider_status import main_mini_app_status


async def _status(token: str) -> dict[str, object]:
    async with Bot(token=token) as bot:
        return main_mini_app_status(bot.bot)


def main() -> int:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        print(json.dumps({"error": "BOT_TOKEN is required"}, sort_keys=True))
        return 1

    try:
        status = asyncio.run(_status(token))
    except Exception:
        print(json.dumps({"error": "telegram provider check failed"}, sort_keys=True))
        return 1

    print(json.dumps(status, ensure_ascii=False, sort_keys=True))
    return 0 if status["has_main_web_app"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
