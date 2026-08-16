from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import telegram_challenge_controller as controller
from question_identity import get_qid


ROOT = Path(__file__).resolve().parents[1]
CONTROLLER_SOURCE = (ROOT / "telegram_challenge_controller.py").read_text(encoding="utf-8")


def test_regular_restart_stays_in_focused_controller_and_uses_canonical_pool(monkeypatch):
    user_id = 42
    chat_id = 314
    questions = [
        {"question": "Q1", "options": ["a", "b"], "correct": 0},
        {"question": "Q2", "options": ["c", "d"], "correct": 1},
        {"question": "Q3", "options": ["e", "f"], "correct": 0},
    ]
    selected = questions[:2]
    session = {
        "mode": "timed",
        "level_key": "regular_pool",
        "level_name": "Regular test",
        "time_limit": 30,
        "questions_data": [{}, {}],
    }
    resolved = SimpleNamespace(
        session=session,
        session_id="session-1",
        attempt_id="attempt-1",
    )
    captured: dict[str, object] = {}

    async def run_blocking(function, *args, **kwargs):
        return function(*args, **kwargs)

    def resolve(callback_data, action, owner_id):
        assert callback_data == "rst:session-1:attempt-1"
        assert action == "rst"
        assert owner_id == user_id
        return resolved

    def restart(session_id, owner_id, **kwargs):
        captured.update(kwargs)
        assert session_id == "session-1"
        assert owner_id == user_id
        return {"session": {"attempt_id": "attempt-2"}}

    def hydrate(owner_id, persisted, **kwargs):
        assert owner_id == user_id
        assert persisted == {"attempt_id": "attempt-2"}
        assert kwargs == {
            "chat_id": chat_id,
            "username": "tester",
            "first_name": "Test",
        }
        return {"level_name": "Regular test", "is_challenge": False}

    def sample(pool, count):
        assert pool is questions
        assert count == 2
        return selected

    monkeypatch.setattr(controller.quiz, "_run_blocking_io", run_blocking)
    monkeypatch.setattr(controller, "resolve_session_action", resolve)
    monkeypatch.setattr(controller, "persisted_is_retry", lambda _session: False)
    monkeypatch.setattr(
        controller,
        "get_pool_by_key",
        lambda key: questions if key == "regular_pool" else [],
    )
    monkeypatch.setattr(controller.random, "sample", sample)
    monkeypatch.setattr(controller, "restart_owned_quiz_attempt", restart)
    monkeypatch.setattr(controller.quiz, "_hydrate_session", hydrate)

    send_regular = AsyncMock()
    send_challenge = AsyncMock()
    monkeypatch.setattr(controller.quiz, "send_question", send_regular)
    monkeypatch.setattr(controller.quiz, "send_challenge_question", send_challenge)

    query = SimpleNamespace(
        data="rst:session-1:attempt-1",
        from_user=SimpleNamespace(id=user_id, username="tester", first_name="Test"),
        message=SimpleNamespace(chat_id=chat_id),
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    update = SimpleNamespace(callback_query=query)
    context = SimpleNamespace(bot=object())

    asyncio.run(controller.restart_session_handler(update, context))

    assert captured == {
        "expected_attempt_id": "attempt-1",
        "mode": "timed",
        "question_ids": [get_qid(item) for item in selected],
        "questions_data": selected,
        "level_key": "regular_pool",
        "level_name": "Regular test",
        "time_limit": 30,
        "chat_id": chat_id,
    }
    query.answer.assert_awaited_once_with()
    query.edit_message_text.assert_awaited_once()
    send_regular.assert_awaited_once_with(context.bot, user_id)
    send_challenge.assert_not_awaited()


def test_focused_restart_no_longer_delegates_to_legacy_controller_restart():
    assert "quiz.restart_session_handler" not in CONTROLLER_SOURCE
    assert "from questions import get_pool_by_key, pick_competitive_challenge_questions" in CONTROLLER_SOURCE
    assert "question_ids=[get_qid(item) for item in questions]" in CONTROLLER_SOURCE
