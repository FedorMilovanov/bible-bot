from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOT = (ROOT / "bot.py").read_text(encoding="utf-8")
SESSION_INTEGRITY = (ROOT / "session_integrity.py").read_text(encoding="utf-8")


def test_bot_imports_session_integrity_before_runtime_handlers():
    assert "from session_integrity import (" in BOT


def test_session_integrity_bootstraps_state_aware_retention_synchronously():
    assert "from legacy_session_retention import ensure_state_aware_session_ttl" in SESSION_INTEGRITY
    assert "from legacy_delivery_retention import ensure_state_aware_delivery_ttl" in SESSION_INTEGRITY
    assert "SESSION_RETENTION_READY = ensure_state_aware_session_ttl()" in SESSION_INTEGRITY
    assert "DELIVERY_RETENTION_READY = ensure_state_aware_delivery_ttl()" in SESSION_INTEGRITY
