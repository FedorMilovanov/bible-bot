"""Canonical achievement catalog and transitional legacy identity bridge."""
from __future__ import annotations

from collections.abc import Mapping


ACHIEVEMENTS = {
    "first_steps": {
        "name": "Первые шаги",
        "icon": "⭐",
        "description": "Пройди свой первый тест",
        "reward": 10,
    },
    "perfectionist_1": {
        "name": "Перфекционист I",
        "icon": "💎",
        "description": "100% в любом тесте",
        "reward": 25,
        "requirement": {"perfect_count": 1},
    },
    "perfectionist_2": {
        "name": "Перфекционист II",
        "icon": "💎💎",
        "description": "100% в 5 тестах",
        "reward": 50,
        "requirement": {"perfect_count": 5},
    },
    "perfectionist_3": {
        "name": "Перфекционист III",
        "icon": "💎💎💎",
        "description": "100% в 15 тестах",
        "reward": 100,
        "requirement": {"perfect_count": 15},
    },
    "streak_5": {
        "name": "Огненная серия",
        "icon": "🔥",
        "description": "5 правильных подряд",
        "reward": 15,
        "requirement": {"max_streak": 5},
    },
    "streak_10": {
        "name": "Снайпер",
        "icon": "🎯",
        "description": "10 правильных подряд",
        "reward": 30,
        "requirement": {"max_streak": 10},
    },
    "streak_20": {
        "name": "Легенда",
        "icon": "👑",
        "description": "20 правильных подряд",
        "reward": 75,
        "requirement": {"max_streak": 20},
    },
    "marathoner_10": {
        "name": "Бегун",
        "icon": "🏃",
        "description": "Пройди 10 тестов",
        "reward": 20,
        "requirement": {"total_tests": 10},
    },
    "marathoner_50": {
        "name": "Марафонец",
        "icon": "🏅",
        "description": "Пройди 50 тестов",
        "reward": 50,
        "requirement": {"total_tests": 50},
    },
    "marathoner_100": {
        "name": "Ультрамарафонец",
        "icon": "🏆",
        "description": "Пройди 100 тестов",
        "reward": 100,
        "requirement": {"total_tests": 100},
    },
    "lightning": {
        "name": "Молния",
        "icon": "⚡",
        "description": "Ответь за 3 сек в скоростном режиме",
        "reward": 20,
    },
    "daily_streak_7": {
        "name": "Неделя знаний",
        "icon": "📅",
        "description": "Проходи тесты 7 дней подряд",
        "reward": 30,
        "requirement": {"daily_streak": 7},
    },
    "daily_streak_30": {
        "name": "Месяц мудрости",
        "icon": "📆",
        "description": "Проходи тесты 30 дней подряд",
        "reward": 100,
        "requirement": {"daily_streak": 30},
    },
}

_REQUIRED_FIELDS = frozenset({"name", "icon", "description", "reward"})
_DEFAULT_CATALOG = object()


def validate_achievement_catalog(catalog=_DEFAULT_CATALOG) -> Mapping[str, Mapping]:
    """Validate one achievement catalog and return it unchanged."""
    candidate = ACHIEVEMENTS if catalog is _DEFAULT_CATALOG else catalog
    if not isinstance(candidate, Mapping) or not candidate:
        raise RuntimeError("Achievement catalog is unavailable")

    for key, achievement in candidate.items():
        if not isinstance(key, str) or not key:
            raise RuntimeError("Achievement catalog contains an invalid key")
        if not isinstance(achievement, Mapping):
            raise RuntimeError(f"Achievement {key!r} is not a mapping")
        missing = _REQUIRED_FIELDS - set(achievement)
        if missing:
            raise RuntimeError(
                f"Achievement {key!r} is missing required fields: {sorted(missing)}"
            )
        reward = achievement["reward"]
        if not isinstance(reward, int) or isinstance(reward, bool) or reward < 0:
            raise RuntimeError(f"Achievement {key!r} has an invalid reward")
        requirement = achievement.get("requirement")
        if requirement is not None and not isinstance(requirement, Mapping):
            raise RuntimeError(f"Achievement {key!r} has an invalid requirement")
    return candidate


def install_legacy_bridge(legacy_module) -> None:
    """Point transitional ``bot.py`` achievement reads at canonical authority."""
    existing = getattr(legacy_module, "ACHIEVEMENTS", None)
    validate_achievement_catalog(existing)
    if existing is ACHIEVEMENTS:
        return
    if existing != ACHIEVEMENTS:
        raise RuntimeError("Legacy achievement catalog diverged from canonical catalog")
    legacy_module.ACHIEVEMENTS = ACHIEVEMENTS


validate_achievement_catalog()
