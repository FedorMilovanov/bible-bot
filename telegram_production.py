"""Single production composition root for Telegram quiz, reports and durable PvP."""
from __future__ import annotations

import importlib
import logging
import os

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)

from keep_alive import keep_alive


def _import_legacy_presentation():
    """Import legacy presentation helpers without starting HTTP as a side effect."""
    previous = os.environ.get("DISABLE_WEB_SERVER")
    os.environ["DISABLE_WEB_SERVER"] = "true"
    try:
        return importlib.import_module("bot")
    finally:
        if previous is None:
            os.environ.pop("DISABLE_WEB_SERVER", None)
        else:
            os.environ["DISABLE_WEB_SERVER"] = previous


legacy = _import_legacy_presentation()

import telegram_admin_controller as admin  # noqa: E402
import telegram_battle_controller as battles  # noqa: E402
import telegram_battle_create_controller as battle_create  # noqa: E402
import telegram_battle_share_controller as battle_share  # noqa: E402
import telegram_broadcast_controller as broadcasts  # noqa: E402
import telegram_controller as quiz  # noqa: E402
import telegram_report_controller as reports  # noqa: E402
import telegram_retry_controller as retry  # noqa: E402
import telegram_settings_controller as settings  # noqa: E402
from broadcast_index_safety import ensure_broadcast_indexes  # noqa: E402
from legacy_session_access import ensure_active_session_unique_index  # noqa: E402
from web_api.db_hardening import (  # noqa: E402
    MiniAppIndexSafetyUnavailable,
    ensure_miniapp_indexes,
)
from web_api.telegram_transport import (  # noqa: E402
    run_telegram_application,
    validate_telegram_transport_configuration,
)

logger = logging.getLogger(__name__)


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Required production environment variable is missing: {name}")
    return value


async def _start(update, context):
    if await battle_share.handle_start_deep_link(update, context):
        return
    await quiz.start(update, context)


async def _reset_during_report(update, context):
    """Drop only the process-local report draft, then run the durable-safe quiz reset."""
    legacy.report_drafts.pop(update.effective_user.id, None)
    return await quiz.reset_command(update, context)


def _touch_presentation_callback(update):
    query = update.callback_query
    legacy._touch(query.from_user.id)
    return query


async def _about_callback(update, context):
    del context
    query = _touch_presentation_callback(update)
    await query.answer()
    await query.edit_message_text(
        "Bible quiz bot: 1 Peter\n"
        "Interactive study tool.\n\n"
        "Found a question issue? Use the in-quiz report action.\n\n"
        "v4.0 - Soli Deo Gloria",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("Back to menu", callback_data="back_to_main")]]
        ),
    )


async def _start_test_callback(update, context):
    query = _touch_presentation_callback(update)
    await query.answer()
    await legacy.choose_level(update, context, is_callback=True)


async def _leaderboard_callback(update, context):
    del context
    query = _touch_presentation_callback(update)
    await query.answer()
    await legacy.show_general_leaderboard(query, 0)


async def _leaderboard_page_callback(update, context):
    del context
    query = _touch_presentation_callback(update)
    await query.answer()
    try:
        page = int((query.data or "").removeprefix("leaderboard_page_"))
    except (TypeError, ValueError):
        return
    await legacy.show_general_leaderboard(query, page)


async def _my_stats_callback(update, context):
    del context
    query = _touch_presentation_callback(update)
    await query.answer()
    await legacy.show_my_stats(query)


async def _achievements_callback(update, context):
    _touch_presentation_callback(update)
    await legacy.show_achievements(update, context)


async def _coming_soon_callback(update, context):
    del context
    query = _touch_presentation_callback(update)
    await query.answer("Coming soon.", show_alert=True)


def main() -> None:
    token = _required_env("BOT_TOKEN")
    _required_env("MONGO_URL")
    validate_telegram_transport_configuration()
    ensure_active_session_unique_index()
    if ensure_miniapp_indexes() is not True:
        raise MiniAppIndexSafetyUnavailable("Mini App index safety is unavailable")
    ensure_broadcast_indexes()

    app = (
        Application.builder()
        .token(token)
        .post_shutdown(quiz._save_all_sessions)
        .build()
    )
    if app.job_queue is None:
        raise RuntimeError(
            "python-telegram-bot JobQueue is required for recovery delivery and maintenance"
        )

    report_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(reports.report_start, pattern="^report_start_")
        ],
        states={
            reports.REPORT_TEXT: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    reports.report_receive_text,
                ),
                CallbackQueryHandler(reports.report_cancel, pattern="^report_cancel$"),
            ],
            reports.REPORT_PHOTO: [
                MessageHandler(filters.PHOTO, reports.report_receive_photo),
                CallbackQueryHandler(
                    reports.report_skip_photo,
                    pattern="^report_skip_photo$",
                ),
                CallbackQueryHandler(reports.report_cancel, pattern="^report_cancel$"),
            ],
            reports.REPORT_CONFIRM: [
                CallbackQueryHandler(reports.report_confirm, pattern="^report_confirm$"),
                CallbackQueryHandler(reports.report_cancel, pattern="^report_cancel$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancelreport", reports.cancel_report_command),
            CommandHandler("reset", _reset_during_report),
        ],
        allow_reentry=True,
    )
    app.add_handler(report_conv)

    app.add_handler(CommandHandler("start", _start))
    app.add_handler(CommandHandler("menu", quiz.start))
    app.add_handler(CommandHandler("test", quiz.test_command))
    app.add_handler(CommandHandler("stats", legacy.stats_command))
    app.add_handler(CommandHandler("random", quiz.random_command))
    app.add_handler(CommandHandler("reset", quiz.reset_command))
    app.add_handler(CommandHandler("cancel", quiz.cancel))
    app.add_handler(CommandHandler("status", quiz.status_command))
    app.add_handler(CommandHandler("cancelreport", reports.cancel_report_command))
    app.add_handler(CommandHandler("admin", legacy.admin_command))
    app.add_handler(CommandHandler("broadcast", broadcasts.broadcast_command))
    app.add_handler(CommandHandler("help", legacy.help_command))

    app.add_handler(CallbackQueryHandler(legacy.level_selected, pattern=r"^level_"))
    app.add_handler(CallbackQueryHandler(retry.retry_errors, pattern="^retry_errors_"))
    app.add_handler(CallbackQueryHandler(quiz.challenge_start, pattern="^challenge_start_"))
    app.add_handler(CallbackQueryHandler(quiz.quiz_inline_answer, pattern=r"^qa:"))
    app.add_handler(CallbackQueryHandler(quiz.challenge_inline_answer, pattern=r"^cha:"))
    app.add_handler(CallbackQueryHandler(quiz.cancel_quiz_handler, pattern="^cancel_quiz$"))

    app.add_handler(CallbackQueryHandler(battles.battle_answer, pattern=r"^bq:"))
    app.add_handler(CallbackQueryHandler(battles.start_battle_questions, pattern=r"^start_battle_"))
    app.add_handler(CallbackQueryHandler(battle_create.create_battle, pattern="^create_battle$"))
    app.add_handler(CallbackQueryHandler(battles.join_battle, pattern="^join_battle_"))
    app.add_handler(CallbackQueryHandler(battles.cancel_battle, pattern="^cancel_battle_"))
    app.add_handler(CallbackQueryHandler(battles.show_battle_menu, pattern="^battle_menu$"))

    app.add_handler(CallbackQueryHandler(legacy.confirm_level_handler, pattern=r"^confirm_level_"))
    app.add_handler(CallbackQueryHandler(quiz.relaxed_mode_handler, pattern=r"^relaxed_mode_"))
    app.add_handler(CallbackQueryHandler(quiz.timed_mode_handler, pattern=r"^timed_mode_"))
    app.add_handler(CallbackQueryHandler(quiz.speed_mode_handler, pattern=r"^speed_mode_"))

    app.add_handler(CallbackQueryHandler(quiz.resume_session_handler, pattern=r"^res:"))
    app.add_handler(CallbackQueryHandler(quiz.restart_session_handler, pattern=r"^rst:"))
    app.add_handler(CallbackQueryHandler(quiz.cancel_session_handler, pattern=r"^can:"))

    app.add_handler(
        CallbackQueryHandler(
            reports.report_inaccuracy_handler,
            pattern=r"^report_inaccuracy_",
        )
    )
    app.add_handler(CallbackQueryHandler(admin.admin_cleanup, pattern=r"^admin_cleanup$"))
    app.add_handler(
        CallbackQueryHandler(
            admin.admin_read_callback,
            pattern=r"^admin_(hard_questions|active_sessions|broadcast_prompt|back)$",
        )
    )

    app.add_handler(CallbackQueryHandler(legacy.back_to_main, pattern="^back_to_main$"))
    app.add_handler(CallbackQueryHandler(legacy.chapter_1_menu, pattern="^chapter_1_menu$"))
    app.add_handler(CallbackQueryHandler(quiz.random_all_start_handler, pattern="^random_all_start$"))
    app.add_handler(CallbackQueryHandler(legacy.historical_menu, pattern="^historical_menu$"))
    app.add_handler(CallbackQueryHandler(legacy.challenge_menu, pattern="^challenge_menu$"))
    app.add_handler(CallbackQueryHandler(legacy.intro_hint_handler, pattern=r"^intro_hint_"))
    app.add_handler(CallbackQueryHandler(quiz.intro_start_handler, pattern=r"^intro_start_"))
    app.add_handler(CallbackQueryHandler(legacy.random_fact_handler, pattern="^random_fact_intro$"))
    app.add_handler(CallbackQueryHandler(legacy.report_menu, pattern="^report_menu$"))
    app.add_handler(CallbackQueryHandler(legacy.challenge_rules, pattern="^challenge_rules_"))
    app.add_handler(CallbackQueryHandler(legacy.show_weekly_leaderboard, pattern="^weekly_lb_"))
    app.add_handler(
        CallbackQueryHandler(
            legacy.category_leaderboard_handler,
            pattern="^cat_lb_",
        )
    )
    app.add_handler(CallbackQueryHandler(settings.user_settings_handler, pattern="^user_settings$"))
    app.add_handler(
        CallbackQueryHandler(
            settings.set_typewriter_handler,
            pattern=r"^typewriter_set:[01]$",
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            settings.legacy_toggle_upgrade_handler,
            pattern="^toggle_typewriter$",
        )
    )
    app.add_handler(CallbackQueryHandler(legacy.show_history, pattern="^my_history$"))
    app.add_handler(CallbackQueryHandler(legacy.review_errors_handler, pattern=r"^review_errors_"))
    app.add_handler(CallbackQueryHandler(legacy.review_errors_handler, pattern=r"^review_nav_"))
    app.add_handler(CallbackQueryHandler(legacy.review_test_handler, pattern=r"^review_test_\d+$"))
    app.add_handler(CallbackQueryHandler(legacy.noop_handler, pattern="^noop$"))

    app.add_handler(CallbackQueryHandler(quiz.show_status_inline, pattern="^my_status$"))
    app.add_handler(CallbackQueryHandler(quiz.reset_session_inline, pattern="^reset_session$"))
    app.add_handler(CallbackQueryHandler(_about_callback, pattern="^about$"))
    app.add_handler(CallbackQueryHandler(_start_test_callback, pattern="^start_test$"))
    app.add_handler(CallbackQueryHandler(_leaderboard_callback, pattern="^leaderboard$"))
    app.add_handler(
        CallbackQueryHandler(_leaderboard_page_callback, pattern=r"^leaderboard_page_\d+$")
    )
    app.add_handler(CallbackQueryHandler(_my_stats_callback, pattern="^my_stats$"))
    app.add_handler(CallbackQueryHandler(_achievements_callback, pattern="^achievements$"))
    app.add_handler(CallbackQueryHandler(_coming_soon_callback, pattern="^coming_soon$"))

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            quiz._general_message_fallback,
        )
    )

    app.job_queue.run_repeating(
        legacy.cleanup_stale_userdata_job,
        interval=legacy.GC_INTERVAL,
        first=legacy.GC_INTERVAL,
    )
    app.job_queue.run_repeating(
        quiz.remind_unfinished_tests_job,
        interval=7200,
        first=7200,
    )
    app.job_queue.run_repeating(
        reports.report_delivery_job,
        interval=60,
        first=10,
    )
    app.job_queue.run_repeating(
        broadcasts.broadcast_delivery_job,
        interval=2,
        first=1,
    )
    app.job_queue.run_repeating(
        battles.battle_maintenance_job,
        interval=60,
        first=10,
    )

    app.add_error_handler(legacy.on_error)

    keep_alive()
    logger.info("Production Telegram composition root started")
    run_telegram_application(
        app,
        webhook_before_shutdown=quiz._save_all_sessions,
    )


if __name__ == "__main__":
    main()
