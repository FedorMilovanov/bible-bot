from pathlib import Path

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


def test_canonical_achievement_catalog_has_expected_contract():
    validated = catalog.validate_achievement_catalog()

    assert validated is catalog.ACHIEVEMENTS
    assert set(validated) == EXPECTED_KEYS
    assert validated["first_steps"]["reward"] == 10
    assert validated["perfectionist_3"]["reward"] == 100
    assert validated["daily_streak_30"]["requirement"] == {"daily_streak": 30}


def test_catalog_validation_rejects_drifted_shapes():
    with pytest.raises(RuntimeError, match="unavailable"):
        catalog.validate_achievement_catalog(None)

    malformed = dict(catalog.ACHIEVEMENTS)
    malformed["first_steps"] = {"name": "broken"}
    with pytest.raises(RuntimeError, match="missing required fields"):
        catalog.validate_achievement_catalog(malformed)

    invalid_reward = dict(catalog.ACHIEVEMENTS)
    invalid_reward["first_steps"] = dict(catalog.ACHIEVEMENTS["first_steps"], reward=True)
    with pytest.raises(RuntimeError, match="invalid reward"):
        catalog.validate_achievement_catalog(invalid_reward)


def test_catalog_has_no_retired_monolith_bridge():
    source = Path("achievement_catalog.py").read_text(encoding="utf-8")
    assert "install_legacy_bridge" not in source
    assert "bot.py" not in source


def test_production_runtime_uses_canonical_achievement_catalog_directly():
    production = Path("telegram_production.py").read_text(encoding="utf-8")
    runtime = Path("telegram_quiz_runtime_controller.py").read_text(encoding="utf-8")

    assert "from achievement_catalog import ACHIEVEMENTS" in runtime
    assert "install_legacy_bridge" not in production
    assert "legacy =" not in production
