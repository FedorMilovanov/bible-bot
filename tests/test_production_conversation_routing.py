import os
from pathlib import Path

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
        'app.add_handler(CallbackQueryHandler(quiz.challenge_start, pattern="^challenge_start_"))'
        in SOURCE
    )
    assert 'CallbackQueryHandler(quiz.quiz_inline_answer, pattern=r"^qa:")' in SOURCE
    assert 'CallbackQueryHandler(quiz.challenge_inline_answer, pattern=r"^cha:")' in SOURCE


def test_report_text_state_accepts_the_cancel_button_it_renders():
    report_text_block = SOURCE.split("reports.REPORT_TEXT:", 1)[1].split(
        "reports.REPORT_PHOTO:", 1
    )[0]
    assert 'CallbackQueryHandler(reports.report_cancel, pattern="^report_cancel$")' in report_text_block


def test_production_module_imports_for_routing_contract():
    assert production.main is not None
