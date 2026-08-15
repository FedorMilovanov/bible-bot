from __future__ import annotations

import course_catalog
import questions
from course_catalog import SURFACE_MINIAPP, SURFACE_TELEGRAM
from questions.pool_policy import SCORING_MODE_LEARNING, get_pool_policy


def _synthetic_pool(chapter: str) -> list[dict]:
    return [
        {
            "id": f"synthetic-{chapter}-{index}",
            "question": f"Synthetic {chapter} question {index}",
            "options": ["a", "b", "c", "d"],
            "correct": 0,
            "explanation": "synthetic integration fixture",
        }
        for index in range(10)
    ]


def _course_keys(surface: str) -> set[str]:
    return {entry.key for entry in course_catalog.list_courses(surface=surface)}


def test_synthetic_chapter4_and_5_auto_surface_without_competitive_widening(monkeypatch):
    competitive_before = [item["id"] for item in questions.COMPETITIVE_POOL]
    battle_before = [item["id"] for item in questions.BATTLE_POOL]
    challenge_before = {
        key: [item["id"] for item in pool]
        for key, pool in questions.CHALLENGE_POOLS.items()
    }
    random_before = [item["id"] for item in questions.get_pool_by_key("random_all")]

    chapter4 = _synthetic_pool("chapter4")
    chapter5 = _synthetic_pool("chapter5")
    # POOL_REGISTRY is the canonical runtime registry object used by
    # get_pool_by_key(). Mutate it in place to model real Chapter authoring
    # registration; rebinding only the exported alias would be an artificial
    # state production registration never creates.
    monkeypatch.setitem(questions.POOL_REGISTRY, "chapter4", chapter4)
    monkeypatch.setitem(questions.POOL_REGISTRY, "chapter5", chapter5)

    for surface in (SURFACE_TELEGRAM, SURFACE_MINIAPP):
        keys = _course_keys(surface)
        assert {"chapter4", "chapter5"} <= keys

    for chapter, synthetic in (("chapter4", chapter4), ("chapter5", chapter5)):
        entry = course_catalog.resolve_course(
            chapter,
            surface=SURFACE_MINIAPP,
            mode="relaxed",
        )
        assert entry.pool_key == chapter
        assert course_catalog.resolve_course_pool(entry) == synthetic

        policy = get_pool_policy(chapter)
        assert policy.scoring_mode == SCORING_MODE_LEARNING
        assert policy.ranked is False
        assert policy.points_per_question == 0

        public = next(
            course
            for group in course_catalog.public_catalog(surface=SURFACE_MINIAPP)["groups"]
            for course in group["courses"]
            if course["key"] == chapter
        )
        assert public["scoring_mode"] == "learning"
        assert public["points_per_question"] == 0
        assert "pool_key" not in public
        assert "ranked" not in public

    assert [item["id"] for item in questions.COMPETITIVE_POOL] == competitive_before
    assert [item["id"] for item in questions.BATTLE_POOL] == battle_before
    assert {
        key: [item["id"] for item in pool]
        for key, pool in questions.CHALLENGE_POOLS.items()
    } == challenge_before
    assert [item["id"] for item in questions.get_pool_by_key("random_all")] == random_before


def test_chapter3_full_learning_pool_remains_165_with_exactly_12_authorized_competitive_cards():
    learning = questions.get_pool_by_key("chapter3")
    competitive = questions.CHAPTER3_AUTHORIZED_COMPETITIVE_POOL
    authorized_ids = set(questions.CHAPTER3_RANKING_AUTHORIZED_IDS)

    assert len(learning) == 165
    assert len(competitive) == 12
    assert len(authorized_ids) == 12
    assert {item["id"] for item in competitive} == authorized_ids
    assert authorized_ids <= {item["id"] for item in learning}

    policy = get_pool_policy("chapter3")
    assert policy.scoring_mode == SCORING_MODE_LEARNING
    assert policy.ranked is False
    assert policy.points_per_question == 0
