from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def async_function(name: str) -> str:
    marker = f"async def {name}"
    start = SOURCE.index(marker)
    next_async = SOURCE.find("\nasync def ", start + len(marker))
    next_sync = SOURCE.find("\ndef ", start + len(marker))
    candidates = [item for item in (next_async, next_sync) if item != -1]
    end = min(candidates) if candidates else len(SOURCE)
    return SOURCE[start:end]


def test_production_root_wires_semantic_durable_battle_handlers():
    required = (
        "import telegram_battle_controller as battles",
        "battles.battle_answer",
        'pattern=r"^bq:"',
        "battles.start_battle_questions",
        "battles.join_battle",
        "battles.cancel_battle",
        "battles.show_battle_menu",
        "battles.battle_maintenance_job",
    )
    for marker in required:
        assert marker in SOURCE


def test_production_root_wires_exact_durable_battle_sharing():
    required = (
        "import telegram_battle_share_controller as battle_share",
        "battle_share.handle_start_deep_link",
        'CommandHandler("start", _start)',
        "battle_share.create_battle",
        'pattern="^create_battle$"',
    )
    for marker in required:
        assert marker in SOURCE

    start = async_function("_start")
    assert start.index("battle_share.handle_start_deep_link") < start.index(
        "await quiz.start(update, context)"
    )


def test_production_root_does_not_register_legacy_or_nonsharing_battle_create_paths():
    forbidden = (
        "legacy.start_battle_questions",
        "legacy.battle_answer",
        'pattern=r"^ba_',
        "legacy.create_battle",
        "legacy.join_battle",
        "legacy.cancel_battle",
        "legacy.cleanup_old_battles_job",
        'CallbackQueryHandler(battles.create_battle, pattern="^create_battle$")',
        "InlineQueryHandler",
        "battle_inline_",
    )
    for marker in forbidden:
        assert marker not in SOURCE


def test_production_root_keeps_quiz_report_and_retry_adapters_explicit():
    required = (
        "import telegram_controller as quiz",
        "import telegram_report_controller as reports",
        "import telegram_retry_controller as retry",
        "quiz.quiz_inline_answer",
        "quiz.challenge_inline_answer",
        "retry.retry_errors",
        'pattern="^retry_errors_"',
        "reports.report_inaccuracy_handler",
        "reports.report_confirm",
        "reports.report_delivery_job",
    )
    for marker in required:
        assert marker in SOURCE


def test_production_root_does_not_register_ram_retry_source_handler():
    assert "quiz.retry_errors" not in SOURCE
    assert "legacy.retry_errors" not in SOURCE


def test_broad_legacy_callback_routers_are_absent_from_production():
    assert "legacy.button_handler" not in SOURCE
    assert "legacy.admin_callback_handler" not in SOURCE
    assert 'CallbackQueryHandler(battles.show_battle_menu, pattern="^battle_menu$")' in SOURCE
    assert 'CallbackQueryHandler(quiz.reset_session_inline, pattern="^reset_session$")' in SOURCE
    assert "admin.admin_read_callback" in SOURCE
