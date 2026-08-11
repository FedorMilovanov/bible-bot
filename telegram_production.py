"""Single production composition root for Telegram quiz, reports and durable PvP."""
from __future__ import annotations

import logging
import os

from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)

import bot as legacy
import telegram_admin_controller as admin
import telegram_battle_controller as battles
import telegram_battle_share_controller as battle_share
import telegram_controller as quiz
import telegram_report_controller as reports
import telegram_retry_controller as retry
from legacy_session_access import ensure_active_session_unique_index
from web_api.db_hardening import MiniAppIndexSafetyUnavailable, ensure_miniapp_indexes

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


def main() -> None:
    token = _required_env("BOT_TOKEN")
    _required_env("MONGO_URL")
    ensure_active_session_unique_index()
    if ensure_miniapp_indexes() is not True:
        raise MiniAppIndexSafetyUnavailable("Mini App index safety is unavailable")

    app = (
        Application.builder()
        .token(token)
        .post_shutdown(quiz._save_all_sessions)
        .build()
    )

    quiz_conv = ConversationHandler(
        entry_points=[
            CommandHandler("test", quiz.test_command),
            CallbackQueryHandler(legacy.level_selected, pattern="^level_"),
            CallbackQueryHandler(retry.retry_errors, pattern="^retry_errors_"),
            CallbackQueryHandler(quiz.challenge_start, pattern="^challenge_start_"),
        ],
        states={
            quiz.CHOOSING_LEVEL: [CallbackQueryHandler(legacy.level_selected)],
            quiz.ANSWERING: [
                CallbackQueryHandler(quiz.cancel_quiz_handler, pattern="^cancel_quiz$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, quiz.text_answer_fallback),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", quiz.cancel),
            CallbackQueryHandler(quiz.cancel_quiz_handler, pattern="^cancel_quiz$"),
            CallbackQueryHandler(legacy.back_to_main, pattern="^back_to_main$"),
        ],
        allow_reentry=True,
    )
    app.add_handler(quiz_conv)

    app.add_handler(CommandHandler("start", _start))
    app.add_handler(CommandHandler("menu", quiz.start))
    app.add_handler(CommandHandler("stats", legacy.stats_command))
    app.add_handler(CommandHandler("random", quiz.random_command))
    app.add_handler(CommandHandler("reset", quiz.reset_command))
    app.add_handler(CommandHandler("status", quiz.status_command))
    app.add_handler(CommandHandler("cancelreport", reports.cancel_report_command))
    app.add_handler(CommandHandler("admin", legacy.admin_command))
    app.add_handler(CommandHandler("broadcast", legacy.broadcast_command))
    app.add_handler(CommandHandler("help", legacy.help_command))

    app.add_handler(CallbackQueryHandler(quiz.quiz_inline_answer, pattern=r"^qa:"))
    app.add_handler(CallbackQueryHandler(quiz.challenge_inline_answer, pattern=r"^cha:"))
    app.add_handler(CallbackQueryHandler(quiz.cancel_quiz_handler, pattern="^cancel_quiz$"))

    app.add_handler(CallbackQueryHandler(battles.battle_answer, pattern=r"^bq:"))
    app.add_handler(CallbackQueryHandler(battles.start_battle_questions, pattern=r"^start_battle_"))
    app.add_handler(CallbackQueryHandler(battle_share.create_battle, pattern="^create_battle$"))
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
            legacy.admin_callback_handler,
            pattern=r"^admin_(hard_questions|active_sessions|broadcast_prompt|back)$",
        )
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
                )
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
            CommandHandler("reset", quiz.reset_command),
        ],
        allow_reentry=True,
    )
    app.add_handler(report_conv)

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
    app.add_handler(CallbackQueryHandler(legacy.user_settings_handler, pattern="^user_settings$"))
    app.add_handler(
        CallbackQueryHandler(
            legacy.toggle_typewriter_handler,
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
    app.add_handler(
        CallbackQueryHandler(
            legacy.button_handler,
            pattern=(
                r"^(about|start_test|leaderboard|my_stats|"
                r"leaderboard_page_\d+|coming_soon|achievements)$"
            ),
        )
    )

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            quiz._general_message_fallback,
        )
    )

    if app.job_queue is not None:
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
            battles.battle_maintenance_job,
            interval=60,
            first=10,
        )

    app.add_error_handler(legacy.on_error)
    logger.info("Production Telegram composition root started")
    app.run_polling()


if __name__ == "__main__":
    main()
