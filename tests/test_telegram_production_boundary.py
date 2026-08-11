from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_production_root_wires_semantic_durable_battle_handlers():
    required = (
        "import telegram_battle_controller as battles",
        "battles.battle_answer",
        'pattern=r"^bq:"',
        "battles.start_battle_questions",
        "battles.create_battle",
        "battles.join_battle",
        "battles.cancel_battle",
        "battles.show_battle_menu",
        "battles.battle_maintenance_job",
    )
    for marker in required:
        assert marker in SOURCE


def test_production_root_does_not_register_legacy_ram_battle_writers():
    forbidden = (
        "legacy.start_battle_questions",
        "legacy.battle_answer",
        'pattern=r"^ba_',
        "legacy.create_battle",
        "legacy.join_battle",
        "legacy.cancel_battle",
        "legacy.cleanup_old_battles_job",
        "InlineQueryHandler",
    )
    for marker in forbidden:
        assert marker not in SOURCE


def test_production_root_keeps_quiz_and_report_adapters_explicit():
    required = (
        "import telegram_controller as quiz",
        "import telegram_report_controller as reports",
        "quiz.quiz_inline_answer",
        "quiz.challenge_inline_answer",
        "reports.report_inaccuracy_handler",
        "reports.report_confirm",
        "reports.report_delivery_job",
    )
    for marker in required:
        assert marker in SOURCE


def test_generic_legacy_button_router_cannot_capture_battle_menu():
    assert 'r"^(about|start_test|leaderboard|my_stats|"' in SOURCE
    assert 'r"^(about|start_test|battle_menu|' not in SOURCE
