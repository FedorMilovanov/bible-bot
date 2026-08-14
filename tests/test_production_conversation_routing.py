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
    assert 'app.add_handler(CommandHandler("test", courses.choose_level))' in SOURCE
    assert (
        'app.add_handler(CallbackQueryHandler(courses.legacy_level_callback, pattern=r"^level_"))'
        in SOURCE
    )
    assert (
        'app.add_handler(CallbackQueryHandler(courses.course_mode_callback, pattern=r"^course_mode:"))'
        in SOURCE
    )
    assert "CallbackQueryHandler(legacy.level_selected" not in SOURCE
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


def test_learning_navigation_routes_through_catalog_surface_not_legacy_authority():
    assert "import telegram_course_surface as courses" in SOURCE
    assert "legacy.choose_level =" not in SOURCE
    assert "legacy.LEVEL_CONFIG" not in SOURCE
    assert "courses.start_course_deep_link" in SOURCE
    assert "CallbackQueryHandler(legacy.chapter_1_menu" not in SOURCE
    assert "CallbackQueryHandler(legacy.historical_menu" not in SOURCE


def test_start_course_deep_link_is_handled_before_legacy_controller(monkeypatch):
    calls = []

    async def no_battle(_update, _context):
        return False

    async def handle_course(_update, _context, key):
        calls.append(("course", key))
        return True

    async def forbidden_legacy_start(_update, _context):
        raise AssertionError("catalog course deep link reached legacy quiz.start")

    monkeypatch.setattr(production.battle_share, "handle_start_deep_link", no_battle)
    monkeypatch.setattr(production.courses, "start_course_deep_link", handle_course)
    monkeypatch.setattr(production.quiz, "start", forbidden_legacy_start)
    context = SimpleNamespace(args=["chapter3"])

    asyncio.run(production._start(SimpleNamespace(), context))

    assert calls == [("course", "chapter3")]
    assert context.args == ["chapter3"]


def test_unknown_start_token_cannot_reach_legacy_level_config_branch(monkeypatch):
    seen = []

    async def no_battle(_update, _context):
        return False

    async def unknown_course(_update, _context, _key):
        return False

    async def safe_legacy_start(_update, context):
        seen.append(list(context.args or []))

    monkeypatch.setattr(production.battle_share, "handle_start_deep_link", no_battle)
    monkeypatch.setattr(production.courses, "start_course_deep_link", unknown_course)
    monkeypatch.setattr(production.quiz, "start", safe_legacy_start)
    context = SimpleNamespace(args=["stale-chapter-token"])

    asyncio.run(production._start(SimpleNamespace(), context))

    assert seen == [[]]
    assert context.args == ["stale-chapter-token"]


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
