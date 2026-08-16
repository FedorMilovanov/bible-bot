from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

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


def test_report_state_bridge_remains_available_for_standalone_compatibility():
    legacy_labels = dict(report_state.REPORT_TYPE_LABELS)
    legacy = SimpleNamespace(
        REPORT_TYPE=report_state.REPORT_TYPE,
        REPORT_TEXT=report_state.REPORT_TEXT,
        REPORT_PHOTO=report_state.REPORT_PHOTO,
        REPORT_CONFIRM=report_state.REPORT_CONFIRM,
        REPORT_TYPE_LABELS=legacy_labels,
        report_drafts={},
    )

    report_state.install_legacy_bridge(legacy)

    assert legacy.REPORT_TYPE == report_state.REPORT_TYPE
    assert legacy.REPORT_TEXT == report_state.REPORT_TEXT
    assert legacy.REPORT_PHOTO == report_state.REPORT_PHOTO
    assert legacy.REPORT_CONFIRM == report_state.REPORT_CONFIRM
    assert legacy.REPORT_TYPE_LABELS is report_state.REPORT_TYPE_LABELS
    assert legacy.report_drafts is report_state.report_drafts
