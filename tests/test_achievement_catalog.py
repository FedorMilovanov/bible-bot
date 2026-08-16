from pathlib import Path
from types import SimpleNamespace

import pytest

import achievement_catalog as catalog


EXPECTED_KEYS = {
    "first_steps",
    "perfectionist_1",
    "perfectionist_2",
    "perfectionist_3",
    "streak_5",
    "streak_10",
    "streak_20",
    "marathoner_10",
    "marathoner_50",
    "marathoner_100",
    "lightning",
    "daily_streak_7",
    "daily_streak_30",
}


def _legacy_copy():
    return {
        key: {
            field: (dict(value) if field == "requirement" else value)
            for field, value in meta.items()
        }
        for key, meta in catalog.ACHIEVEMENTS.items()
    }


def test_canonical_achievement_catalog_has_expected_contract():
    validated = catalog.validate_achievement_catalog()

    assert validated is catalog.ACHIEVEMENTS
    assert set(validated) == EXPECTED_KEYS
    assert validated["first_steps"]["reward"] == 10
    assert validated["perfectionist_3"]["reward"] == 100
    assert validated["daily_streak_30"]["requirement"] == {"daily_streak": 30}


def test_legacy_bridge_preserves_values_and_replaces_identity():
    legacy = SimpleNamespace(ACHIEVEMENTS=_legacy_copy())
    assert legacy.ACHIEVEMENTS is not catalog.ACHIEVEMENTS

    catalog.install_legacy_bridge(legacy)

    assert legacy.ACHIEVEMENTS is catalog.ACHIEVEMENTS


def test_legacy_bridge_fails_closed_on_catalog_drift():
    drifted = _legacy_copy()
    drifted["first_steps"]["reward"] = 999
    legacy = SimpleNamespace(ACHIEVEMENTS=drifted)

    with pytest.raises(RuntimeError, match="diverged"):
        catalog.install_legacy_bridge(legacy)


def test_legacy_bridge_fails_closed_on_malformed_catalog():
    with pytest.raises(RuntimeError):
        catalog.install_legacy_bridge(SimpleNamespace(ACHIEVEMENTS=None))


def test_production_no_longer_installs_achievement_legacy_bridge():
    source = Path("telegram_production.py").read_text(encoding="utf-8")

    assert "achievement_catalog.install_legacy_bridge" not in source
    assert "import achievement_catalog as achievement_catalog" not in source
    assert "import bot" not in source
    assert "_import_legacy_presentation" not in source
