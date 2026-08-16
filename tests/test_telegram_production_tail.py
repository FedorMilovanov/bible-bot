import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from telegram.error import BadRequest, NetworkError

import telegram_error_controller as errors
import telegram_report_runtime as report_runtime
import telegram_report_state as report_state


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
REPORT_RUNTIME_SOURCE = (ROOT / "telegram_report_runtime.py").read_text(encoding="utf-8")
REPORT_CONTROLLER_SOURCE = (ROOT / "telegram_report_controller.py").read_text(encoding="utf-8")
REPORT_STATE_SOURCE = (ROOT / "telegram_report_state.py").read_text(encoding="utf-8")


def test_report_runtime_drops_canonical_mapping():
    report_state.report_drafts.clear()
    report_state.report_drafts.update(
        {7: {"report_id": "r1"}, 8: {"report_id": "r2"}}
    )

    assert report_runtime.report_drafts is report_state.report_drafts
    assert report_runtime.drop_report_draft(7) is True
    assert report_runtime.drop_report_draft(7) is False
    assert report_state.report_drafts == {8: {"report_id": "r2"}}
    report_state.report_drafts.clear()


def test_report_state_is_direct_canonical_owner_without_legacy_bridge():
    assert report_runtime.report_drafts is report_state.report_drafts
    assert "install_legacy_bridge" not in REPORT_STATE_SOURCE
    assert "legacy_module" not in REPORT_STATE_SOURCE


def test_report_runtime_and_controller_have_no_legacy_module_import():
    assert "import bot" not in REPORT_RUNTIME_SOURCE
    assert "legacy." not in REPORT_RUNTIME_SOURCE
    assert "from telegram_report_state import report_drafts" in REPORT_RUNTIME_SOURCE

    assert "import bot" not in REPORT_CONTROLLER_SOURCE
    assert "legacy." not in REPORT_CONTROLLER_SOURCE
    assert "get_active_quiz_session_strict" in REPORT_CONTROLLER_SOURCE


def test_error_policy_ignores_network_noise():
    bot = SimpleNamespace(send_message=AsyncMock())
    context = SimpleNamespace(error=NetworkError("network"), bot=bot)

    asyncio.run(errors.on_error(None, context, admin_user_id=11))

    bot.send_message.assert_not_awaited()


def test_error_policy_ignores_not_modified():
    bot = SimpleNamespace(send_message=AsyncMock())
    context = SimpleNamespace(
        error=BadRequest("Message is not modified"),
        bot=bot,
    )

    asyncio.run(errors.on_error(None, context, admin_user_id=11))

    bot.send_message.assert_not_awaited()


def test_error_policy_notifies_user_and_admin(monkeypatch):
    class FakeUpdate:
        def __init__(self, message):
            self.effective_user = SimpleNamespace(id=5)
            self.message = message
            self.callback_query = None

    monkeypatch.setattr(errors, "Update", FakeUpdate)
    message = SimpleNamespace(reply_text=AsyncMock())
    bot = SimpleNamespace(send_message=AsyncMock())
    context = SimpleNamespace(error=RuntimeError("boom"), bot=bot)

    asyncio.run(
        errors.on_error(
            FakeUpdate(message),
            context,
            admin_user_id=99,
        )
    )

    message.reply_text.assert_awaited_once()
    assert "Произошла ошибка" in message.reply_text.await_args.args[0]
    markup = message.reply_text.await_args.kwargs["reply_markup"]
    assert [button.callback_data for button in markup.inline_keyboard[0]] == [
        "reset_session",
        "report_start_bug_direct",
    ]

    bot.send_message.assert_awaited_once()
    kwargs = bot.send_message.await_args.kwargs
    assert kwargs["chat_id"] == 99
    assert "RuntimeError: boom" in kwargs["text"]


def test_error_handler_factory_binds_admin_id():
    handler = errors.build_error_handler(42)
    bot = SimpleNamespace(send_message=AsyncMock())
    context = SimpleNamespace(error=NetworkError("network"), bot=bot)

    asyncio.run(handler(None, context))
    bot.send_message.assert_not_awaited()


def test_production_tail_routes_outside_direct_legacy_surface():
    assert "import telegram_error_controller as errors" in PRODUCTION_SOURCE
    assert "import telegram_report_runtime as report_runtime" in PRODUCTION_SOURCE
    assert "report_runtime.drop_report_draft" in PRODUCTION_SOURCE
    assert "errors.build_error_handler" in PRODUCTION_SOURCE

    assert "import telegram_report_state as report_state" not in PRODUCTION_SOURCE
    assert "report_state.install_legacy_bridge" not in PRODUCTION_SOURCE
    assert "legacy.report_drafts" not in PRODUCTION_SOURCE
    assert "legacy.on_error" not in PRODUCTION_SOURCE
    assert "legacy =" not in PRODUCTION_SOURCE
