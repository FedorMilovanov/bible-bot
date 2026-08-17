# ruff: noqa: RUF001
"""Canonical production runtime authority for the Telegram main menu."""
from __future__ import annotations

from collections.abc import Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo

MiniAppUrlProvider = Callable[[], str | None]
_miniapp_url_provider: MiniAppUrlProvider | None = None

_BASE_BUTTONS = (
    ("📖 О боте", "about"),
    ("🎯 Начать тест", "start_test"),
    ("🎲 Челлендж (20) — бонус", "challenge_menu"),
    ("🏛 Исторический контекст", "historical_menu"),
    ("⚔️ Режим битвы", "battle_menu"),
    ("🏆 Таблица лидеров", "leaderboard"),
    ("📊 Моя статистика", "my_stats"),
    ("📌 Мой статус", "my_status"),
    ("✉️ Обратная связь", "report_menu"),
    ("⚙️ Настройки", "user_settings"),
)


def configure_miniapp_url_provider(provider: MiniAppUrlProvider | None) -> None:
    """Configure the optional Mini App URL source used by every main-menu render."""
    if provider is not None and not callable(provider):
        raise TypeError("Mini App URL provider must be callable or None")
    global _miniapp_url_provider
    _miniapp_url_provider = provider


def current_miniapp_url() -> str | None:
    """Return the normalized configured Mini App URL, if one is available."""
    if _miniapp_url_provider is None:
        return None
    value = _miniapp_url_provider()
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def main_keyboard() -> InlineKeyboardMarkup:
    """Render the production main menu from one canonical button declaration."""
    rows = []
    url = current_miniapp_url()
    if url:
        rows.append(
            [
                InlineKeyboardButton(
                    "🚀 Открыть приложение",
                    web_app=WebAppInfo(url=url),
                )
            ]
        )
    rows.extend(
        [
            [InlineKeyboardButton(label, callback_data=callback)]
            for label, callback in _BASE_BUTTONS
        ]
    )
    return InlineKeyboardMarkup(rows)
