"""Canonical product catalog for normal learning courses.

The catalog owns course presentation metadata and course -> canonical pool
mapping. It never imports raw question-bank modules and never decides question
competitive admission. Pool existence comes from ``questions.POOL_REGISTRY``;
scoring semantics come from ``questions.pool_policy``.

A catalog declaration whose pool is missing, too small, or lacks product policy
is *unavailable*. Public surfaces filter unavailable entries, while direct/stale
requests fail closed through ``resolve_course``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from questions.pool_policy import PoolPolicy, get_pool_policy

SURFACE_TELEGRAM = "telegram"
SURFACE_MINIAPP = "miniapp"
KNOWN_SURFACES = frozenset({SURFACE_TELEGRAM, SURFACE_MINIAPP})
DEFAULT_MODES = ("relaxed", "timed", "speed")


class CourseCatalogError(ValueError):
    """Base error for invalid or unavailable course requests."""


class UnknownCourseError(CourseCatalogError):
    pass


class CourseUnavailableError(CourseCatalogError):
    pass


class CourseModeNotAllowedError(CourseCatalogError):
    pass


@dataclass(frozen=True, slots=True)
class CourseGroup:
    key: str
    title: str
    description: str
    icon: str
    order: int
    home_card: bool = True

    def public_dict(self) -> dict:
        return {
            "key": self.key,
            "title": self.title,
            "description": self.description,
            "icon": self.icon,
            "order": self.order,
            "home_card": self.home_card,
        }


@dataclass(frozen=True, slots=True)
class CourseEntry:
    key: str
    title: str
    description: str
    pool_key: str
    default_question_count: int
    group: str
    order: int
    surfaces: frozenset[str]
    allowed_modes: tuple[str, ...] = DEFAULT_MODES
    icon: str = "📖"
    legacy_callback_key: str | None = None

    def __post_init__(self) -> None:
        if not self.key or not self.pool_key or not self.title:
            raise ValueError("course key, pool key and title are required")
        if ":" in self.key:
            raise ValueError(f"course key cannot contain callback separator: {self.key!r}")
        if isinstance(self.default_question_count, bool) or self.default_question_count <= 0:
            raise ValueError("default_question_count must be positive")
        if not self.surfaces or not self.surfaces.issubset(KNOWN_SURFACES):
            raise ValueError(f"invalid surfaces for course {self.key!r}")
        if not self.allowed_modes or len(set(self.allowed_modes)) != len(self.allowed_modes):
            raise ValueError(f"invalid allowed_modes for course {self.key!r}")
        unknown_modes = sorted(set(self.allowed_modes) - set(DEFAULT_MODES))
        if unknown_modes:
            raise ValueError(f"unknown allowed_modes for course {self.key!r}: {unknown_modes}")


COURSE_GROUPS: tuple[CourseGroup, ...] = (
    CourseGroup(
        "chapter1",
        "Глава 1",
        "Уровни от фактов до лингвистики",
        "📖",
        10,
    ),
    CourseGroup(
        "chapter2",
        "Глава 2",
        "Исследовательский курс · без рейтинга",
        "📘",
        20,
    ),
    CourseGroup(
        "chapter3",
        "Глава 3",
        "Reviewed-курс · без рейтинга",
        "📙",
        30,
    ),
    CourseGroup(
        "chapter4",
        "Глава 4",
        "Учебный курс",
        "📗",
        40,
    ),
    CourseGroup(
        "chapter5",
        "Глава 5",
        "Учебный курс",
        "📕",
        50,
    ),
    CourseGroup(
        "context",
        "Контекст",
        "Нерон · география · введение",
        "🏛",
        60,
    ),
)

_BOTH = frozenset({SURFACE_TELEGRAM, SURFACE_MINIAPP})
_TG = frozenset({SURFACE_TELEGRAM})

# The order is declarative and deterministic. Chapter 4/5 are intentionally
# predeclared: until questions.POOL_REGISTRY contains their pool, they disappear
# from every exposed surface and cannot be started.
COURSE_ENTRIES: tuple[CourseEntry, ...] = (
    CourseEntry("level_easy", "🟢 Легкий уровень (ст. 1–25)", "Вся 1 глава", "easy", 10, "chapter1", 10, _TG, legacy_callback_key="level_easy"),
    CourseEntry("level_easy_p1", "🟢 Легкий (ст. 1–16)", "Первая часть главы", "easy_p1", 10, "chapter1", 11, _BOTH, legacy_callback_key="level_easy_p1"),
    CourseEntry("level_easy_p2", "🟢 Легкий (ст. 17–25)", "Вторая часть главы", "easy_p2", 10, "chapter1", 12, _BOTH, legacy_callback_key="level_easy_p2"),
    CourseEntry("level_medium", "🟡 Средний (ст. 1–25)", "Вся 1 глава", "medium", 10, "chapter1", 20, _TG, legacy_callback_key="level_medium"),
    CourseEntry("level_medium_p1", "🟡 Средний (ст. 1–16)", "Первая часть главы", "medium_p1", 10, "chapter1", 21, _BOTH, legacy_callback_key="level_medium_p1"),
    CourseEntry("level_medium_p2", "🟡 Средний (ст. 17–25)", "Вторая часть главы", "medium_p2", 10, "chapter1", 22, _BOTH, legacy_callback_key="level_medium_p2"),
    CourseEntry("level_hard", "🔴 Сложный (ст. 1–25)", "Вся 1 глава", "hard", 10, "chapter1", 30, _TG, legacy_callback_key="level_hard"),
    CourseEntry("level_hard_p1", "🔴 Сложный (ст. 1–16)", "Первая часть главы", "hard_p1", 10, "chapter1", 31, _BOTH, legacy_callback_key="level_hard_p1"),
    CourseEntry("level_hard_p2", "🔴 Сложный (ст. 17–25)", "Вторая часть главы", "hard_p2", 10, "chapter1", 32, _BOTH, legacy_callback_key="level_hard_p2"),
    CourseEntry("level_practical_ch1", "🙏 Применение (ст. 1–25)", "Практика жизни", "practical_ch1", 10, "chapter1", 40, _TG, legacy_callback_key="level_practical_ch1"),
    CourseEntry("level_practical_p1", "🙏 Применение (ст. 1–16)", "Практика жизни", "practical_p1", 10, "chapter1", 41, _BOTH, legacy_callback_key="level_practical_p1"),
    CourseEntry("level_practical_p2", "🙏 Применение (ст. 17–25)", "Практика жизни", "practical_p2", 10, "chapter1", 42, _BOTH, legacy_callback_key="level_practical_p2"),
    CourseEntry("level_linguistics_ch1", "🔬 Лингвистика: Избранные и странники", "Греческий текст · часть 1", "linguistics_ch1", 10, "chapter1", 50, _BOTH, legacy_callback_key="level_linguistics_ch1"),
    CourseEntry("level_linguistics_ch1_2", "🔬 Лингвистика: Живая надежда", "Греческий текст · часть 2", "linguistics_ch1_2", 10, "chapter1", 51, _BOTH, legacy_callback_key="level_linguistics_ch1_2"),
    CourseEntry("level_linguistics_ch1_3", "🔬 Лингвистика: Искупление и истина", "Греческий текст · часть 3", "linguistics_ch1_3", 10, "chapter1", 52, _BOTH, legacy_callback_key="level_linguistics_ch1_3"),
    CourseEntry("level_random_all", "🎲 Случайный режим (все темы)", "Legacy Chapter 1/context random learning pool", "random_all", 10, "chapter1", 90, _TG, ("relaxed",), "🎲", "level_random_all"),
    CourseEntry("level_intro1", "📜 Введение: Авторство ч.1", "Исторический контекст", "intro1", 10, "context", 10, _BOTH, legacy_callback_key="level_intro1"),
    CourseEntry("level_intro2", "📜 Введение: Авторство ч.2", "Исторический контекст", "intro2", 10, "context", 20, _BOTH, legacy_callback_key="level_intro2"),
    CourseEntry("level_intro3", "📜 Введение: Структура и цель", "Исторический контекст", "intro3", 10, "context", 30, _BOTH, legacy_callback_key="level_intro3"),
    CourseEntry("level_nero", "👑 Правление Нерона", "Исторический контекст", "nero", 10, "context", 40, _BOTH, legacy_callback_key="level_nero"),
    CourseEntry("level_geography", "🌍 География земли", "Исторический контекст", "geography", 10, "context", 50, _BOTH, legacy_callback_key="level_geography"),
    CourseEntry(
        "chapter2",
        "📘 1 Петра — Глава 2",
        "Текст · греческий · ВЗ/LXX · история · богословие · применение",
        "chapter2",
        10,
        "chapter2",
        10,
        _BOTH,
        icon="📘",
    ),
    CourseEntry(
        "chapter3",
        "📙 1 Петра — Глава 3",
        "Reviewed-банк: текст · греческий · ВЗ/LXX · история · богословие · спорные места · применение",
        "chapter3",
        10,
        "chapter3",
        10,
        _BOTH,
        icon="📙",
    ),
    CourseEntry(
        "chapter4",
        "📗 1 Петра — Глава 4",
        "Учебный курс по 1 Петра 4",
        "chapter4",
        10,
        "chapter4",
        10,
        _BOTH,
        icon="📗",
    ),
    CourseEntry(
        "chapter5",
        "📕 1 Петра — Глава 5",
        "Учебный курс по 1 Петра 5",
        "chapter5",
        10,
        "chapter5",
        10,
        _BOTH,
        icon="📕",
    ),
)


def _validate_catalog(entries: Iterable[CourseEntry] = COURSE_ENTRIES) -> None:
    entries = tuple(entries)
    keys = [entry.key for entry in entries]
    if len(keys) != len(set(keys)):
        duplicates = sorted({key for key in keys if keys.count(key) > 1})
        raise ValueError(f"duplicate course keys: {duplicates}")
    group_keys = {group.key for group in COURSE_GROUPS}
    unknown_groups = sorted({entry.group for entry in entries} - group_keys)
    if unknown_groups:
        raise ValueError(f"unknown course groups: {unknown_groups}")


_validate_catalog()
_BY_KEY = {entry.key: entry for entry in COURSE_ENTRIES}
_GROUP_BY_KEY = {group.key: group for group in COURSE_GROUPS}


def _pool_registry() -> dict[str, list[dict]]:
    from questions import POOL_REGISTRY

    return POOL_REGISTRY


def course_policy(entry: CourseEntry) -> PoolPolicy:
    return get_pool_policy(entry.pool_key)


def course_available(entry: CourseEntry) -> bool:
    try:
        pool = _pool_registry().get(entry.pool_key)
        course_policy(entry)
    except Exception:
        return False
    return isinstance(pool, list) and len(pool) >= entry.default_question_count


def resolve_course(
    course_key: str,
    *,
    surface: str | None = None,
    mode: str | None = None,
) -> CourseEntry:
    key = str(course_key or "").strip()
    entry = _BY_KEY.get(key)
    if entry is None:
        raise UnknownCourseError(f"unknown course: {key!r}")
    if surface is not None:
        if surface not in KNOWN_SURFACES:
            raise CourseCatalogError(f"unknown surface: {surface!r}")
        if surface not in entry.surfaces:
            raise CourseUnavailableError(f"course {key!r} is not exposed on {surface}")
    if mode is not None and mode not in entry.allowed_modes:
        raise CourseModeNotAllowedError(f"mode {mode!r} is not allowed for course {key!r}")
    if not course_available(entry):
        raise CourseUnavailableError(f"course {key!r} is unavailable")
    return entry


def resolve_course_pool(entry: CourseEntry) -> list[dict]:
    # Recheck availability immediately before handing a pool to a start path.
    resolved = resolve_course(entry.key)
    from questions import get_pool_by_key

    return get_pool_by_key(resolved.pool_key)


def list_courses(*, surface: str, group: str | None = None) -> list[CourseEntry]:
    if surface not in KNOWN_SURFACES:
        raise CourseCatalogError(f"unknown surface: {surface!r}")
    entries = [
        entry
        for entry in COURSE_ENTRIES
        if surface in entry.surfaces
        and (group is None or entry.group == group)
        and course_available(entry)
    ]
    return sorted(entries, key=lambda entry: (entry.order, entry.key))


def list_groups(*, surface: str) -> list[CourseGroup]:
    available_groups = {entry.group for entry in list_courses(surface=surface)}
    return [
        group
        for group in sorted(COURSE_GROUPS, key=lambda item: (item.order, item.key))
        if group.key in available_groups
    ]


def public_course(entry: CourseEntry) -> dict:
    policy = course_policy(entry)
    return {
        "key": entry.key,
        "title": entry.title,
        "description": entry.description,
        "icon": entry.icon,
        "group": entry.group,
        "order": entry.order,
        "default_question_count": entry.default_question_count,
        "modes": list(entry.allowed_modes),
        "scoring_mode": policy.scoring_mode,
        "points_per_question": policy.points_per_question,
    }


def public_catalog(*, surface: str) -> dict:
    groups = []
    for group in list_groups(surface=surface):
        item = group.public_dict()
        item["courses"] = [
            public_course(entry)
            for entry in list_courses(surface=surface, group=group.key)
        ]
        groups.append(item)
    return {"version": 1, "groups": groups}


def legacy_level_config(*, surface: str = SURFACE_TELEGRAM) -> dict[str, dict]:
    """Catalog-derived compatibility view for transitional legacy call sites."""
    result: dict[str, dict] = {}
    for entry in list_courses(surface=surface):
        policy = course_policy(entry)
        config = {
            "pool_key": entry.pool_key,
            "name": entry.title,
            "points_per_q": policy.points_per_question,
            "num_questions": entry.default_question_count,
        }
        result[entry.key] = config
        if entry.legacy_callback_key:
            result[entry.legacy_callback_key] = config
    return result


def course_for_pool(pool_key: str, *, surface: str) -> CourseEntry | None:
    matches = [
        entry
        for entry in list_courses(surface=surface)
        if entry.pool_key == pool_key
    ]
    if len(matches) == 1:
        return matches[0]
    return None


__all__ = [
    "COURSE_ENTRIES",
    "COURSE_GROUPS",
    "CourseCatalogError",
    "CourseEntry",
    "CourseGroup",
    "CourseModeNotAllowedError",
    "CourseUnavailableError",
    "DEFAULT_MODES",
    "KNOWN_SURFACES",
    "SURFACE_MINIAPP",
    "SURFACE_TELEGRAM",
    "UnknownCourseError",
    "course_available",
    "course_for_pool",
    "course_policy",
    "legacy_level_config",
    "list_courses",
    "list_groups",
    "public_catalog",
    "public_course",
    "resolve_course",
    "resolve_course_pool",
]
