from __future__ import annotations

from types import SimpleNamespace

import pytest

import telegram_controller_legacy_bridge as bridge
from course_catalog import legacy_level_config
from telegram_conversation_states import ANSWERING, BATTLE_ANSWERING, CHOOSING_LEVEL


def _legacy(**overrides):
    canonical = legacy_level_config()["level_linguistics_ch1"]
    level_config = {
        "level_linguistics_ch1": {
            "pool_key": canonical["pool_key"],
            # Deliberately preserve historical deployed wording rather than the
            # newer catalog title. Presentation copy is not routing authority.
            "name": "🔬 Лингвистика: Избранные и странники (ч.1)",
            "points_per_q": canonical["points_per_q"],
            "num_questions": canonical["num_questions"],
        }
    }
    values = {
        "CHOOSING_LEVEL": CHOOSING_LEVEL,
        "ANSWERING": ANSWERING,
        "BATTLE_ANSWERING": BATTLE_ANSWERING,
        "LEVEL_CONFIG": level_config,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_bridge_preserves_copy_but_canonicalizes_structural_level_metadata():
    legacy = _legacy()
    old_mapping = legacy.LEVEL_CONFIG

    bridge.install_legacy_bridge(legacy)

    assert legacy.LEVEL_CONFIG is not old_mapping
    assert legacy.LEVEL_CONFIG == {
        "level_linguistics_ch1": {
            "pool_key": "linguistics_ch1",
            "name": "🔬 Лингвистика: Избранные и странники (ч.1)",
            "points_per_q": 3,
            "num_questions": 10,
        }
    }
    assert (
        legacy.CHOOSING_LEVEL,
        legacy.ANSWERING,
        legacy.BATTLE_ANSWERING,
    ) == (CHOOSING_LEVEL, ANSWERING, BATTLE_ANSWERING)


@pytest.mark.parametrize("field,bad_value", [
    ("pool_key", "wrong_pool"),
    ("points_per_q", 99),
    ("num_questions", 99),
])
def test_bridge_fails_closed_on_structural_catalog_drift(field, bad_value):
    legacy = _legacy()
    original = legacy.LEVEL_CONFIG
    legacy.LEVEL_CONFIG["level_linguistics_ch1"][field] = bad_value

    with pytest.raises(RuntimeError, match="diverged from canonical catalog"):
        bridge.install_legacy_bridge(legacy)

    assert legacy.LEVEL_CONFIG is original
    assert legacy.CHOOSING_LEVEL == CHOOSING_LEVEL


def test_bridge_fails_closed_on_unknown_or_extra_legacy_authority():
    unknown = _legacy(LEVEL_CONFIG={"legacy_only": {
        "pool_key": "easy",
        "name": "Legacy only",
        "points_per_q": 1,
        "num_questions": 10,
    }})
    with pytest.raises(RuntimeError, match="unknown course"):
        bridge.install_legacy_bridge(unknown)

    extra = _legacy()
    extra.LEVEL_CONFIG["level_linguistics_ch1"]["ranked"] = True
    with pytest.raises(RuntimeError, match="unsupported fields"):
        bridge.install_legacy_bridge(extra)


def test_bridge_rejects_bad_presentation_name_without_partial_state_mutation():
    legacy = _legacy(CHOOSING_LEVEL=CHOOSING_LEVEL)
    legacy.LEVEL_CONFIG["level_linguistics_ch1"]["name"] = ""
    original = legacy.LEVEL_CONFIG

    with pytest.raises(RuntimeError, match="name is invalid"):
        bridge.install_legacy_bridge(legacy)

    assert legacy.LEVEL_CONFIG is original


def test_bridge_rejects_quiz_state_drift_before_replacing_level_mapping():
    legacy = _legacy(ANSWERING=77)
    original = legacy.LEVEL_CONFIG

    with pytest.raises(RuntimeError, match="conversation states diverged"):
        bridge.install_legacy_bridge(legacy)

    assert legacy.LEVEL_CONFIG is original
    assert legacy.ANSWERING == 77
