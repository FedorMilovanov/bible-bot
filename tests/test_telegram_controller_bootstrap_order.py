from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import telegram_report_state as report_state


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_controller_authority_bridges_install_before_any_controller_capable_import():
    controller_import = PRODUCTION_SOURCE.index("import telegram_controller as quiz")
    first_controller_capable_import = PRODUCTION_SOURCE.index(
        "import telegram_activity_controller as activity"
    )

    required_installs = (
        "controller_legacy_bridge.install_legacy_bridge(legacy)",
        "quiz_runtime.install_legacy_bridge(legacy)",
        "question_identity.install_legacy_bridge(legacy)",
        "answer_history.install_legacy_bridge(legacy)",
        "achievement_catalog.install_legacy_bridge(legacy)",
        "report_state.install_legacy_bridge(legacy)",
    )
    for token in required_installs:
        position = PRODUCTION_SOURCE.index(token)
        assert position < first_controller_capable_import
        assert position < controller_import

    # The main-menu bridge legitimately waits for _miniapp_url to exist; its
    # controller-facing presentation callables are resolved dynamically later.
    assert PRODUCTION_SOURCE.index("def _miniapp_url()") < PRODUCTION_SOURCE.index(
        "main_menu.install_legacy_bridge(legacy, miniapp_url_provider=_miniapp_url)"
    )


def test_report_state_bridge_replaces_exact_canonical_metadata_after_parity():
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
