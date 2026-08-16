import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_production_command_menu_sync_uses_retry_after_adapter():
    assert "import telegram_command_menu_retry as command_menu" in SOURCE
    assert "command_menu.sync_public_commands_once(" in SOURCE
    assert "_sync_public_command_menu_job," in SOURCE

    tree = ast.parse(SOURCE, filename="telegram_production.py")
    run_once_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "run_once"
    ]
    assert any(
        call.args
        and isinstance(call.args[0], ast.Name)
        and call.args[0].id == "_sync_public_command_menu_job"
        and any(
            keyword.arg == "when"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == 0
            for keyword in call.keywords
        )
        for call in run_once_calls
    )
