from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_production_command_menu_sync_uses_retry_after_adapter():
    assert "import telegram_command_menu_retry as command_menu" in SOURCE
    assert "command_menu.sync_public_commands_once(" in SOURCE
    assert "_sync_public_command_menu_job," in SOURCE
    assert "app.job_queue.run_once(\n        _sync_public_command_menu_job,\n        when=0," in SOURCE
