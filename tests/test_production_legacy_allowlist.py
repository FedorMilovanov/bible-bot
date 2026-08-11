import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")

# Transitional legacy dependencies that are intentionally presentation/read-only
# or process-local. Any new legacy attribute must be reviewed explicitly before
# it can enter the production composition root.
ALLOWED_LEGACY_ATTRIBUTES = {
    "GC_INTERVAL",
    "admin_callback_handler",
    "admin_command",
    "back_to_main",
    "broadcast_command",
    "button_handler",
    "category_leaderboard_handler",
    "challenge_menu",
    "challenge_rules",
    "chapter_1_menu",
    "cleanup_stale_userdata_job",
    "confirm_level_handler",
    "help_command",
    "historical_menu",
    "intro_hint_handler",
    "level_selected",
    "noop_handler",
    "on_error",
    "random_fact_handler",
    "report_menu",
    "review_errors_handler",
    "review_test_handler",
    "show_history",
    "show_weekly_leaderboard",
    "stats_command",
    "toggle_typewriter_handler",
    "user_settings_handler",
}


def _legacy_attributes() -> set[str]:
    tree = ast.parse(SOURCE, filename="telegram_production.py")
    return {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "legacy"
    }


def test_production_legacy_surface_is_exactly_allowlisted():
    assert _legacy_attributes() == ALLOWED_LEGACY_ATTRIBUTES


def test_allowlist_excludes_known_state_authority_writers():
    forbidden = {
        "main",
        "test_command",
        "random_command",
        "reset_command",
        "status_command",
        "relaxed_mode_handler",
        "timed_mode_handler",
        "speed_mode_handler",
        "random_all_start_handler",
        "intro_start_handler",
        "retry_errors",
        "cancel_quiz_handler",
        "quiz_inline_answer",
        "challenge_inline_answer",
        "show_results",
        "show_challenge_results",
        "start_battle_questions",
        "battle_answer",
        "create_battle",
        "join_battle",
        "cancel_battle",
        "cleanup_old_battles_job",
        "report_confirm",
        "report_inaccuracy_handler",
        "remind_unfinished_tests_job",
        "_save_all_sessions",
    }
    assert ALLOWED_LEGACY_ATTRIBUTES.isdisjoint(forbidden)
