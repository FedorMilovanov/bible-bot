import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
BOT_SOURCE = (ROOT / "bot.py").read_text(encoding="utf-8")
DATABASE_SOURCE = (ROOT / "database.py").read_text(encoding="utf-8")

# Transitional legacy dependencies that are intentionally presentation/read-only
# or process-local. Any new legacy attribute must be reviewed explicitly before
# it can enter the production composition root.
ALLOWED_LEGACY_ATTRIBUTES = {
    "GC_INTERVAL",
    "admin_callback_handler",
    "admin_command",
    "back_to_main",
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
    "report_drafts",
    "report_menu",
    "review_errors_handler",
    "review_test_handler",
    "show_history",
    "show_weekly_leaderboard",
    "stats_command",
}

_NON_CALLABLE_LEGACY_ATTRIBUTES = {"GC_INTERVAL", "report_drafts"}

# These functions either change durable business authority or implement a
# replay-sensitive process-local toggle. A production-allowed legacy handler
# must never acquire a call path to one of them. This is intentionally broader
# than the exact handler allowlist: it also fences imported database/integrity
# writers that could otherwise be called through an innocent-looking helper.
FORBIDDEN_STATE_WRITER_CALLS = {
    "add_to_leaderboard",
    "advance_quiz_session",
    "battle_answer",
    "broadcast_command",
    "cancel_active_quiz_session",
    "cancel_battle",
    "cancel_quiz_session",
    "challenge_inline_answer",
    "check_daily_bonus",
    "claim_battle_opponent",
    "claim_final_battle",
    "cleanup_old_battles_job",
    "create_battle",
    "create_battle_doc",
    "create_quiz_session",
    "db_cleanup_stale_battles",
    "delete_battle",
    "delete_battle_for_participant",
    "finish_quiz_session",
    "insert_report",
    "join_battle",
    "mark_report_delivered",
    "quiz_inline_answer",
    "record_battle_result",
    "record_question_stat",
    "relaxed_mode_handler",
    "report_confirm",
    "report_inaccuracy_handler",
    "retry_errors",
    "set_pref",
    "set_question_sent_at",
    "show_challenge_results",
    "show_results",
    "speed_mode_handler",
    "start_battle_questions",
    "timed_mode_handler",
    "update_achievement_stats",
    "update_battle",
    "update_battle_stats",
    "update_challenge_stats",
    "update_daily_streak",
    "update_quiz_session",
    "update_weekly_leaderboard",
}

FORBIDDEN_COLLECTION_WRITES = {
    "collection.bulk_write",
    "collection.delete_many",
    "collection.delete_one",
    "collection.find_one_and_delete",
    "collection.find_one_and_replace",
    "collection.find_one_and_update",
    "collection.insert_many",
    "collection.insert_one",
    "collection.replace_one",
    "collection.update_many",
    "collection.update_one",
}

EXPECTED_BUTTON_HANDLER_PATTERN = (
    r"^(about|start_test|leaderboard|my_stats|"
    r"leaderboard_page_\d+|coming_soon|achievements)$"
)


def _legacy_attributes() -> set[str]:
    tree = ast.parse(SOURCE, filename="telegram_production.py")
    return {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "legacy"
    }


def _function_map(source: str, filename: str) -> dict[str, ast.AST]:
    tree = ast.parse(source, filename=filename)
    return {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _expr_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _expr_name(node.value)
        if base:
            return f"{base}.{node.attr}"
    return None


def _direct_call_targets(function_node: ast.AST) -> set[str]:
    targets = set()
    for node in ast.walk(function_node):
        if not isinstance(node, ast.Call):
            continue
        target = _expr_name(node.func)
        if target:
            targets.add(target)
    return targets


def _production_button_handler_patterns() -> list[str]:
    tree = ast.parse(SOURCE, filename="telegram_production.py")
    patterns = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _expr_name(node.func) != "CallbackQueryHandler":
            continue
        if not node.args or _expr_name(node.args[0]) != "legacy.button_handler":
            continue
        pattern_kw = next((kw for kw in node.keywords if kw.arg == "pattern"), None)
        assert pattern_kw is not None
        pattern = ast.literal_eval(pattern_kw.value)
        assert isinstance(pattern, str)
        patterns.append(pattern)
    return patterns


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
        "broadcast_command",
        "toggle_typewriter_handler",
        "user_settings_handler",
        "remind_unfinished_tests_job",
        "_save_all_sessions",
    }
    assert ALLOWED_LEGACY_ATTRIBUTES.isdisjoint(forbidden)


def test_allowed_legacy_handlers_cannot_reach_business_state_writers():
    functions = _function_map(BOT_SOURCE, "bot.py")
    roots = ALLOWED_LEGACY_ATTRIBUTES - _NON_CALLABLE_LEGACY_ATTRIBUTES
    assert roots <= functions.keys()

    violations = []
    for root in sorted(roots):
        pending = [root]
        visited = set()
        while pending:
            current = pending.pop()
            if current in visited:
                continue
            visited.add(current)
            for target in _direct_call_targets(functions[current]):
                if target in FORBIDDEN_STATE_WRITER_CALLS or target in FORBIDDEN_COLLECTION_WRITES:
                    violations.append((root, current, target))
                    continue
                if target in functions and target not in visited:
                    pending.append(target)

    assert violations == []


def test_activity_touch_remains_a_last_activity_only_write():
    functions = _function_map(DATABASE_SOURCE, "database.py")
    touch = functions["touch_user_activity"]
    targets = _direct_call_targets(touch)
    assert "collection.update_one" in targets
    assert targets.isdisjoint(FORBIDDEN_COLLECTION_WRITES - {"collection.update_one"})

    string_constants = {
        node.value
        for node in ast.walk(touch)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    assert {"$set", "last_activity"} <= string_constants
    assert string_constants.isdisjoint({"$inc", "$push", "$addToSet", "$pull", "$unset"})
    assert not any(
        isinstance(node, ast.keyword) and node.arg == "upsert"
        for node in ast.walk(touch)
    )


def test_legacy_button_dispatcher_stays_narrowly_registered():
    assert _production_button_handler_patterns() == [EXPECTED_BUTTON_HANDLER_PATTERN]
