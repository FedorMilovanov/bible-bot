"""Safe, non-secret visibility into Telegram Main Mini App provider state."""
from __future__ import annotations

from typing import Any


def main_mini_app_status(bot_user: Any) -> dict[str, object]:
    """Normalize getMe/PTB User state without exposing credentials."""
    username = str(getattr(bot_user, "username", "") or "").strip().lstrip("@")
    return {
        "username": username,
        "has_main_web_app": bool(getattr(bot_user, "has_main_web_app", False)),
    }
