from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
DATABASE_SOURCE = (ROOT / "database.py").read_text(encoding="utf-8")

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


def test_production_composition_has_no_retired_runtime_authority_surface():
    tree = ast.parse(PRODUCTION_SOURCE, filename="telegram_production.py")
    imported_roots = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_roots.update(
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    )

    assert "bot" not in imported_roots
    assert "telegram_controller" not in imported_roots
    assert "telegram_controller_legacy_bridge" not in imported_roots
    assert "import telegram_quiz_runtime_controller as quiz" in PRODUCTION_SOURCE


def test_production_routes_product_domains_to_focused_owners():
    required = (
        "import telegram_battle_controller as battles",
        "import telegram_broadcast_controller as broadcasts",
        "import telegram_course_surface as courses",
        "import telegram_quiz_runtime_controller as quiz",
        "import telegram_report_controller as reports",
        "import telegram_settings_controller as settings",
    )
    for marker in required:
        assert marker in PRODUCTION_SOURCE

    forbidden_legacy_attributes = (
        "legacy.LEVEL_CONFIG",
        "legacy.admin_callback_handler",
        "legacy.battle_answer",
        "legacy.button_handler",
        "legacy.report_confirm",
        "legacy.show_results",
    )
    for marker in forbidden_legacy_attributes:
        assert marker not in PRODUCTION_SOURCE


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
