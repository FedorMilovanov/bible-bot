from __future__ import annotations

from pathlib import Path

import telegram_report_state as report_state


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_production_needs_no_legacy_bootstrap_order_anymore():
    forbidden = (
        "importlib.import_module(\"bot\")",
        "legacy =",
        "controller_legacy_bridge.install_legacy_bridge",
        "quiz_runtime.install_legacy_bridge",
        "question_identity.install_legacy_bridge",
        "answer_history.install_legacy_bridge",
        "achievement_catalog.install_legacy_bridge",
        "report_state.install_legacy_bridge",
        "main_menu.install_legacy_bridge",
        "import telegram_controller as quiz",
    )
    for token in forbidden:
        assert token not in PRODUCTION_SOURCE

    assert "import telegram_quiz_runtime_controller as quiz" in PRODUCTION_SOURCE
    assert PRODUCTION_SOURCE.index("def _miniapp_url()") < PRODUCTION_SOURCE.index(
        "main_menu.configure_miniapp_url_provider(_miniapp_url)"
    )


def test_report_state_is_owned_directly_without_install_bridge():
    source = Path(report_state.__file__).read_text(encoding="utf-8")
    assert not hasattr(report_state, "install_legacy_bridge")
    assert "install_legacy_bridge" not in source
    assert report_state.REPORT_TYPE_LABELS
    assert isinstance(report_state.report_drafts, dict)
