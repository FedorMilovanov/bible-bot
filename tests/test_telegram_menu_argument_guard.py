from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import telegram_production as production


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_menu_command_uses_argument_guard_instead_of_legacy_quiz_start_directly():
    assert 'app.add_handler(CommandHandler("menu", _menu))' in SOURCE
    assert 'app.add_handler(CommandHandler("menu", quiz.start))' not in SOURCE


def test_menu_arguments_are_hidden_from_legacy_controller_and_restored(monkeypatch):
    seen = []

    async def fake_start(_update, context):
        seen.append(list(context.args or []))
        return "menu-result"

    monkeypatch.setattr(production.quiz, "start", fake_start)
    context = SimpleNamespace(args=["chapter3"])

    result = asyncio.run(production._menu(SimpleNamespace(), context))

    assert result == "menu-result"
    assert seen == [[]]
    assert context.args == ["chapter3"]
