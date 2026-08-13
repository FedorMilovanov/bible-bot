import asyncio
import os
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("ADMIN_USER_ID", "1")
os.environ.setdefault("DISABLE_WEB_SERVER", "true")

import telegram_production as production


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_quiz_routing_is_stateless_at_ptb_composition_layer():
    assert "quiz_conv = ConversationHandler(" not in SOURCE
    assert 'app.add_handler(CommandHandler("test", quiz.test_command))' in SOURCE
    assert (
        'app.add_handler(CallbackQueryHandler(legacy.level_selected, pattern=r"^level_"))'
        in SOURCE
    )
    assert 'app.add_handler(CommandHandler("cancel", quiz.cancel))' in SOURCE


def test_live_quiz_runtime_is_not_owned_by_conversation_state():
    assert "quiz.ANSWERING:" not in SOURCE
    assert (
        'app.add_handler(CallbackQueryHandler(retry.retry_errors, pattern="^retry_errors_"))'
        in SOURCE
    )
    assert (
        'app.add_handler(CallbackQueryHandler(challenge.challenge_start, pattern="^challenge_start_"))'
        in SOURCE
    )
    assert 'CallbackQueryHandler(quiz.quiz_inline_answer, pattern=r"^qa:")' in SOURCE
    assert 'CallbackQueryHandler(quiz.challenge_inline_answer, pattern=r"^cha:")' in SOURCE


def test_report_text_state_accepts_the_cancel_button_it_renders():
    report_text_block = SOURCE.split("reports.REPORT_TEXT:", 1)[1].split(
        "reports.REPORT_PHOTO:", 1
    )[0]
    assert 'CallbackQueryHandler(reports.report_cancel, pattern="^report_cancel$")' in report_text_block


def test_active_report_conversation_precedes_same_group_global_commands():
    report_registration = SOURCE.index("app.add_handler(report_conv)")
    global_cancel = SOURCE.index(
        'app.add_handler(CommandHandler("cancelreport", reports.cancel_report_command))'
    )
    global_reset = SOURCE.index(
        'app.add_handler(CommandHandler("reset", quiz.reset_command))'
    )
    assert report_registration < global_cancel
    assert report_registration < global_reset
    assert 'CommandHandler("reset", _reset_during_report)' in SOURCE


def test_reset_during_report_drops_only_local_draft_before_quiz_reset(monkeypatch):
    calls = []
    production.legacy.report_drafts[42] = {"report_id": "r1"}

    async def fake_reset(update, context):
        calls.append((update, context, 42 in production.legacy.report_drafts))
        return "reset-result"

    monkeypatch.setattr(production.quiz, "reset_command", fake_reset)
    update = SimpleNamespace(effective_user=SimpleNamespace(id=42))
    context = object()

    result = asyncio.run(production._reset_during_report(update, context))

    assert result == "reset-result"
    assert calls == [(update, context, False)]
    assert 42 not in production.legacy.report_drafts


def test_production_module_imports_for_routing_contract():
    assert production.main is not None
