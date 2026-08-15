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
    "_main_keyboard",
    "_touch",
    "admin_command",
    "back_to_main",
    "challenge_menu",
    "challenge_rules",
    "help_command",
    "intro_hint_handler",
    "noop_handler",
    "on_error",
    "random_fact_handler",
    "report_drafts",
    "report_menu",
    "review_errors_handler",
    "review_test_handler",
    "show_achievements",
}

_NON_CALLABLE_LEGACY_ATTRIBUTES = {"report_drafts"}

# Non-database authority writers still need an explicit fence because they live
# in legacy/integrity/controller helpers rather than database.py.
FORBIDDEN_STATE_WRITER_CALLS = {
    "battle_answer",
    "broadcast_command",
    "cancel_battle",
    "challenge_inline_answer",
    "claim_battle_opponent",
    "claim_final_battle",
    "cleanup_old_battles_job",
    "create_battle",
    "db_cleanup_stale_battles",
    "delete_battle_for_participant",
    "join_battle",
    "quiz_inline_answer",
    "record_battle_result",
    "relaxed_mode_handler",
    "report_confirm",
    "report_inaccuracy_handler",
    "retry_errors",
    "set_pref",
    "show_challenge_results",
    "show_results",
    "speed_mode_handler",
    "start_battle_questions",
    "timed_mode_handler",
}

MONGO_MUTATION_METHODS = {
    "bulk_write",
    "delete_many",
    "delete_one",
    "find_one_and_delete",
    "find_one_and_replace",
    "find_one_and_update",
    "insert_many",
    "insert_one",
    "replace_one",
    "update_many",
    "update_one",
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


def _database_writer_functions() -> set[str]:
    functions = _function_map(DATABASE_SOURCE, "database.py")
    writers = set()
    for name, node in functions.items():
        for target in _direct_call_targets(node):
            method = target.rsplit(".", 1)[-1]
            if "." in target and method in MONGO_MUTATION_METHODS:
                writers.add(name)
                break
    return writers


def test_production_legacy_surface_is_exactly_allowlisted():
    assert _legacy_attributes() == ALLOWED_LEGACY_ATTRIBUTES


def test_allowlist_excludes_legacy_learning_menu_and_course_authority():
    forbidden = {
        "LEVEL_CONFIG",
        "chapter_1_menu",
        "choose_level",
        "confirm_level_handler",
        "historical_menu",
        "level_selected",
    }
    assert forbidden.isdisjoint(ALLOWED_LEGACY_ATTRIBUTES)
    for name in forbidden:
        assert f"legacy.{name}" not in SOURCE


def test_allowlist_excludes_known_state_authority_writers_and_broad_dispatchers():
    forbidden = {
        "admin_callback_handler",
        "battle_answer",
        "broadcast_command",
        "button_handler",
        "cancel_battle",
        "cancel_quiz_handler",
        "challenge_inline_answer",
        "cleanup_old_battles_job",
        "create_battle",
        "intro_start_handler",
        "join_battle",
        "main",
        "random_all_start_handler",
        "random_command",
        "relaxed_mode_handler",
        "remind_unfinished_tests_job",
        "report_confirm",
        "report_inaccuracy_handler",
        "reset_command",
        "retry_errors",
        "show_challenge_results",
        "show_results",
        "speed_mode_handler",
        "start_battle_questions",
        "status_command",
        "test_command",
        "timed_mode_handler",
        "toggle_typewriter_handler",
        "user_settings_handler",
        "_save_all_sessions",
    }
    assert ALLOWED_LEGACY_ATTRIBUTES.isdisjoint(forbidden)
    assert "legacy.admin_callback_handler" not in SOURCE
    assert "legacy.button_handler" not in SOURCE


def test_database_writer_detection_covers_known_mutating_helpers():
    writers = _database_writer_functions()
    assert {
        "init_user_stats",
        "add_to_leaderboard",
        "create_quiz_session",
        "update_quiz_session",
        "advance_quiz_session",
        "create_battle_doc",
        "insert_report",
        "touch_user_activity",
    } <= writers


def test_allowed_legacy_handlers_cannot_reach_business_state_writers():
    functions = _function_map(BOT_SOURCE, "bot.py")
    roots = ALLOWED_LEGACY_ATTRIBUTES - _NON_CALLABLE_LEGACY_ATTRIBUTES
    assert roots <= functions.keys()

    # Every database.py function with a Mongo mutation is automatically fenced.
    # The sole exception is touch_user_activity, whose exact last_activity-only
    # shape is proven independently below.
    forbidden_calls = FORBIDDEN_STATE_WRITER_CALLS | (
        _database_writer_functions() - {"touch_user_activity"}
    )

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
                if target in forbidden_calls:
                    violations.append((root, current, target))
                    continue
                if target in functions and target not in visited:
                    pending.append(target)

    assert violations == []


def test_activity_touch_remains_a_last_activity_only_write():
    functions = _function_map(DATABASE_SOURCE, "database.py")
    touch = functions["touch_user_activity"]
    targets = _direct_call_targets(touch)
    mutation_targets = {
        target
        for target in targets
        if "." in target and target.rsplit(".", 1)[-1] in MONGO_MUTATION_METHODS
    }
    assert mutation_targets == {"collection.update_one"}

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
