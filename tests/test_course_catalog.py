from __future__ import annotations

import pytest

import course_catalog
import questions
from course_catalog import (
    COURSE_ENTRIES,
    CourseEntry,
    CourseUnavailableError,
    SURFACE_MINIAPP,
    SURFACE_TELEGRAM,
    course_available,
    course_policy,
    list_courses,
    public_catalog,
    resolve_course,
)
from questions.pool_policy import SCORING_MODE_LEARNING, SCORING_MODE_SCORED


def _keys(surface: str) -> list[str]:
    return [entry.key for entry in list_courses(surface=surface)]


def _entry(key: str) -> CourseEntry:
    return next(item for item in COURSE_ENTRIES if item.key == key)


def test_catalog_schema_has_unique_keys_and_deterministic_order():
    keys = [entry.key for entry in COURSE_ENTRIES]
    assert len(keys) == len(set(keys))

    first = [(entry.order, entry.key) for entry in list_courses(surface=SURFACE_MINIAPP)]
    second = [(entry.order, entry.key) for entry in list_courses(surface=SURFACE_MINIAPP)]
    assert first == second
    assert first == sorted(first)


def test_duplicate_course_keys_are_rejected():
    sample = COURSE_ENTRIES[0]
    duplicate = CourseEntry(
        sample.key,
        "Duplicate",
        "duplicate declaration",
        sample.pool_key,
        sample.default_question_count,
        sample.group,
        sample.order + 1,
        sample.surfaces,
    )
    with pytest.raises(ValueError, match="duplicate course keys"):
        course_catalog._validate_catalog((sample, duplicate))


def test_course_declaration_rejects_unknown_modes_and_callback_separator():
    sample = COURSE_ENTRIES[0]
    with pytest.raises(ValueError, match="unknown allowed_modes"):
        CourseEntry(
            "future",
            "Future",
            "future declaration",
            sample.pool_key,
            sample.default_question_count,
            sample.group,
            sample.order,
            sample.surfaces,
            ("relaxed", "ranked"),
        )
    with pytest.raises(ValueError, match="callback separator"):
        CourseEntry(
            "future:ranked",
            "Future",
            "future declaration",
            sample.pool_key,
            sample.default_question_count,
            sample.group,
            sample.order,
            sample.surfaces,
        )


def test_chapter2_and_chapter3_are_exposed_on_both_learning_surfaces():
    for surface in (SURFACE_TELEGRAM, SURFACE_MINIAPP):
        keys = _keys(surface)
        assert "chapter2" in keys
        assert "chapter3" in keys

    chapter2 = resolve_course("chapter2", surface=SURFACE_MINIAPP)
    chapter3 = resolve_course("chapter3", surface=SURFACE_TELEGRAM)
    assert chapter2.pool_key == "chapter2"
    assert chapter3.pool_key == "chapter3"
    assert chapter2.default_question_count == 10
    assert chapter3.default_question_count == 10


def test_chapter2_and_normal_chapter3_are_learning_only_non_ranked():
    for key in ("chapter2", "chapter3"):
        policy = course_policy(resolve_course(key, surface=SURFACE_MINIAPP))
        assert policy.scoring_mode == SCORING_MODE_LEARNING
        assert policy.ranked is False
        assert policy.points_per_question == 0


def test_chapter3_learning_pool_does_not_replace_competitive_authority():
    learning = questions.get_pool_by_key("chapter3")
    competitive = questions.CHAPTER3_AUTHORIZED_COMPETITIVE_POOL
    assert len(learning) > len(competitive)
    assert len(competitive) == 12
    assert {item["id"] for item in competitive} == set(questions.CHAPTER3_RANKING_AUTHORIZED_IDS)
    assert all(item in learning for item in competitive)


def test_future_chapters_fail_closed_when_canonical_pool_is_missing(monkeypatch):
    fake = dict(questions.POOL_REGISTRY)
    fake.pop("chapter4", None)
    fake.pop("chapter5", None)
    monkeypatch.setattr(course_catalog, "_pool_registry", lambda: fake)

    for key in ("chapter4", "chapter5"):
        entry = _entry(key)
        assert not course_available(entry)
        assert key not in _keys(SURFACE_MINIAPP)
        assert key not in _keys(SURFACE_TELEGRAM)
        with pytest.raises(CourseUnavailableError):
            resolve_course(key, surface=SURFACE_MINIAPP)


def test_future_chapter_surfaces_automatically_when_pool_registry_contains_pool(monkeypatch):
    fake = dict(questions.POOL_REGISTRY)
    fake["chapter4"] = [
        {"id": f"future-{index}", "question": "q", "options": ["a", "b"], "correct": 0}
        for index in range(10)
    ]
    monkeypatch.setattr(course_catalog, "_pool_registry", lambda: fake)

    entry = resolve_course("chapter4", surface=SURFACE_MINIAPP)
    assert entry.pool_key == "chapter4"
    assert "chapter4" in _keys(SURFACE_TELEGRAM)


def test_future_chapter_real_exposure_tracks_current_backend_registry():
    """A/B registration must surface without changing Telegram/Mini App code/tests."""
    for key in ("chapter4", "chapter5"):
        entry = _entry(key)
        pool = questions.POOL_REGISTRY.get(entry.pool_key)
        expected = isinstance(pool, list) and len(pool) >= entry.default_question_count
        assert course_available(entry) is expected
        for surface in (SURFACE_MINIAPP, SURFACE_TELEGRAM):
            assert (key in _keys(surface)) is expected


def test_public_catalog_contains_only_client_product_metadata():
    payload = public_catalog(surface=SURFACE_MINIAPP)
    assert payload["version"] == 1
    chapter3 = next(
        course
        for group in payload["groups"]
        for course in group["courses"]
        if course["key"] == "chapter3"
    )
    assert chapter3["scoring_mode"] == "learning"
    assert chapter3["points_per_question"] == 0

    forbidden = {
        "pool_key",
        "ranked",
        "correct",
        "correct_answer",
        "questions",
        "source",
        "sources",
        "ranking_authorized_ids",
        "persistence_id",
        "session_id",
    }
    assert forbidden.isdisjoint(chapter3)


def test_catalog_policy_matches_legacy_scored_persistence_map():
    import database

    for entry in list_courses(surface=SURFACE_TELEGRAM):
        policy = course_policy(entry)
        if policy.scoring_mode != SCORING_MODE_SCORED:
            assert entry.pool_key not in database.POINTS_PER_QUESTION
            continue
        assert database.POINTS_PER_QUESTION[entry.pool_key] == policy.points_per_question


def test_catalog_usage_does_not_mutate_random_challenge_or_battle_pools():
    random_before = [item["id"] for item in questions.get_pool_by_key("random_all")]
    competitive_before = [item["id"] for item in questions.COMPETITIVE_POOL]
    battle_before = [item["id"] for item in questions.BATTLE_POOL]
    challenge_before = {
        key: [item["id"] for item in pool]
        for key, pool in questions.CHALLENGE_POOLS.items()
    }

    course_catalog.public_catalog(surface=course_catalog.SURFACE_MINIAPP)

    assert [item["id"] for item in questions.get_pool_by_key("random_all")] == random_before
    assert [item["id"] for item in questions.COMPETITIVE_POOL] == competitive_before
    assert [item["id"] for item in questions.BATTLE_POOL] == battle_before
    assert {
        key: [item["id"] for item in pool]
        for key, pool in questions.CHALLENGE_POOLS.items()
    } == challenge_before


def test_catalog_and_telegram_surface_import_without_cycle(monkeypatch):
    monkeypatch.setenv("DISABLE_WEB_SERVER", "true")
    import telegram_course_surface

    assert telegram_course_surface.legacy_level_config()["chapter2"]["pool_key"] == "chapter2"
