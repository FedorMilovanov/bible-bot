import asyncio
import os
from pathlib import Path

os.environ.setdefault("ADMIN_USER_ID", "1")
os.environ.setdefault("DISABLE_WEB_SERVER", "true")

from telegram.ext import ConversationHandler

import telegram_production as production


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_choosing_level_state_matches_only_level_callbacks():
    assert (
        'CallbackQueryHandler(legacy.level_selected, pattern=r"^level_")'
        in SOURCE
    )
    assert "quiz.CHOOSING_LEVEL: [CallbackQueryHandler(legacy.level_selected)]" not in SOURCE


def test_back_to_main_fallback_explicitly_leaves_conversation_state():
    assert 'CallbackQueryHandler(_back_to_main, pattern="^back_to_main$")' in SOURCE


def test_back_to_main_wrapper_renders_menu_then_returns_end(monkeypatch):
    calls = []

    async def fake_back_to_main(update, context):
        calls.append((update, context))

    monkeypatch.setattr(production.legacy, "back_to_main", fake_back_to_main)
    update = object()
    context = object()

    result = asyncio.run(production._back_to_main(update, context))

    assert calls == [(update, context)]
    assert result == ConversationHandler.END
