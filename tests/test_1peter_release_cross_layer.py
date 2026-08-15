from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import database
import telegram_course_surface as telegram_courses
from course_catalog import SURFACE_MINIAPP, SURFACE_TELEGRAM, list_courses
from questions import BATTLE_POOL, CHALLENGE_POOLS, COMPETITIVE_POOL, POOL_REGISTRY
from questions.pool_policy import get_pool_policy
from web_api import quiz_start, result_store


class MemorySessions:
    def __init__(self):
        self.inserted = None

    def find_one(self, _query):
        return None

    def insert_one(self, document):
        self.inserted = document
        return None


class FakeMessage:
    def __init__(self):
        self.replies = []

    async def reply_text(self, text, reply_markup=None, parse_mode=None):
        self.replies.append((text, reply_markup))


def _ids(items):
    return {str(item["id"]) for item in items}


@pytest.mark.parametrize("chapter", ["chapter4", "chapter5"])
def test_miniapp_chapter_start_to_learning_progress_is_server_authoritative(monkeypatch, chapter):
    sessions = MemorySessions()
    monkeypatch.setattr(quiz_start.core, "miniapp_sessions", lambda: sessions)
    monkeypatch.setattr(database, "init_user_stats", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(database, "get_user_stats", lambda user_id: {"_id": str(user_id)})
    monkeypatch.setattr(quiz_start.random, "sample", lambda population, count: list(population)[:count])

    body, message, status = quiz_start.start_quiz(
        {"id": 5100, "username": "release", "first_name": "Release"},
        {"course_key": chapter, "mode": "relaxed"},
    )
    assert status == 200
    assert message is None
    assert body["pool_key"] == chapter
    assert body["challenge"] is False
    assert body["total"] == 10
    assert set(body["question"]) == {"id", "question", "options"}
    assert sessions.inserted["stats_level_key"] == chapter
    assert sessions.inserted["is_challenge"] is False
    assert len(sessions.inserted["questions"]) == 10

    captured = {}

    class Users:
        def find_one(self, query):
            assert query == {"_id": "5100"}
            return {"_id": "5100", "total_points": 999, "total_tests": 77}

    def fake_persist(user_id, result_id, update, receipt):
        captured["update"] = update
        captured["receipt"] = receipt
        return dict(receipt)

    monkeypatch.setattr(result_store, "_user_collection", lambda: Users())
    monkeypatch.setattr(result_store, "_persist_once", fake_persist)
    receipt = result_store._apply_learning_result_once(
        user_id=5100,
        result_id=f"{chapter}-release-attempt",
        username="release",
        first_name="Release",
        level_key=chapter,
        score=8,
        total=10,
    )
    assert receipt == {
        "points": 0,
        "daily_bonus": 0,
        "new_achievements": [],
        "kind": "learning",
        "level_key": chapter,
        "score": 8,
        "total": 10,
    }
    assert captured["update"]["$inc"] == {
        f"{chapter}_attempts": 1,
        f"{chapter}_correct": 8,
        f"{chapter}_total": 10,
    }
    assert captured["update"]["$max"] == {f"{chapter}_best_score": 8}
    assert "total_points" not in captured["update"]["$inc"]
    assert "total_tests" not in captured["update"]["$inc"]


@pytest.mark.parametrize("chapter", ["chapter4", "chapter5"])
def test_client_cannot_turn_release_learning_course_into_scored_mode(chapter):
    for field, value in (
        ("ranked", True),
        ("scoring_mode", "scored"),
        ("points_per_question", 999),
        ("score_multiplier", 99),
    ):
        body, message, status = quiz_start.start_quiz(
            {"id": 5101, "username": "release", "first_name": "Release"},
            {"course_key": chapter, "mode": "relaxed", field: value},
        )
        assert body is None
        assert status == 400
        assert "override server course policy" in message


@pytest.mark.parametrize("chapter", ["chapter4", "chapter5"])
def test_telegram_course_deep_link_uses_same_catalog_and_learning_policy(chapter):
    message = FakeMessage()
    handled = asyncio.run(
        telegram_courses.start_course_deep_link(
            SimpleNamespace(message=message),
            SimpleNamespace(),
            chapter,
        )
    )
    assert handled is True
    assert len(message.replies) == 1
    text, keyboard = message.replies[0]
    assert "без рейтинговых баллов" in text
    callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
    assert f"course_mode:relaxed:{chapter}" in callbacks
    policy = get_pool_policy(chapter)
    assert policy.scoring_mode == "learning"
    assert policy.points_per_question == 0


def test_catalog_surfaces_and_gameplay_boundaries_match_for_chapters2_to5():
    telegram = {entry.key for entry in list_courses(surface=SURFACE_TELEGRAM)}
    miniapp = {entry.key for entry in list_courses(surface=SURFACE_MINIAPP)}
    for chapter in ("chapter2", "chapter3", "chapter4", "chapter5"):
        assert chapter in telegram
        assert chapter in miniapp
        policy = get_pool_policy(chapter)
        assert policy.scoring_mode == "learning"
        assert policy.ranked is False
        assert policy.points_per_question == 0

    learning_ids = set().union(*(_ids(POOL_REGISTRY[key]) for key in ("chapter2", "chapter3", "chapter4", "chapter5")))
    assert _ids(POOL_REGISTRY["random_all"]).isdisjoint(learning_ids)
    chapter45 = _ids(POOL_REGISTRY["chapter4"]) | _ids(POOL_REGISTRY["chapter5"])
    assert _ids(COMPETITIVE_POOL).isdisjoint(chapter45)
    assert _ids(BATTLE_POOL).isdisjoint(chapter45)
    assert all(_ids(pool).isdisjoint(chapter45) for pool in CHALLENGE_POOLS.values())
