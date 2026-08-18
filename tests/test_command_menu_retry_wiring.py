import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_production_command_menu_sync_uses_retry_after_adapter():
    assert "import telegram_command_menu_retry as command_menu" in SOURCE
    assert "command_menu.sync_public_commands_once(" in SOURCE
    assert "_sync_public_command_menu_job," in SOURCE

    tree = ast.parse(SOURCE, filename="telegram_production.py")
    post_init_callbacks = []
    zero_delay_provider_jobs = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr == "post_init" and node.args:
            callback = node.args[0]
            if isinstance(callback, ast.Name):
                post_init_callbacks.append(callback.id)
        if node.func.attr != "run_once" or not node.args:
            continue
        callback = node.args[0]
        if not isinstance(callback, ast.Name) or callback.id != "_sync_public_command_menu_job":
            continue
        if any(
            keyword.arg == "when"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == 0
            for keyword in node.keywords
        ):
            zero_delay_provider_jobs.append(node.lineno)

    assert post_init_callbacks == ["_sync_public_command_menu_post_init"]
    assert zero_delay_provider_jobs == []
