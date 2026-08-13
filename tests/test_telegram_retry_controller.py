import asyncio
import os
from datetime import UTC, datetime
from types import SimpleNamespace

os.environ.setdefault("ADMIN_USER_ID", "1")

import telegram_retry_controller as retry
from legacy_retry_source import (
    LegacyRetrySourceInvalid,
    LegacyRetrySourceUnavailable,
    RetrySource,
)


class _Query:
    def __init__(self, *, user_id=42, data="retry_errors_42"):
        self.from_user = SimpleNamespace(
            id=user_id,
            username="tester",
            first_name="Tester",
        )
        self.data = data
        self.message = SimpleNamespace(
            chat_id=777,
            date=datetime(2026, 8, 11, 18, 0, 10, tzinfo=UTC),
        )
        self.answers = []
        self.edits = []

    async def answer(self, text=None, show_alert=False):
        self.answers.append((text, show_alert))

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


class _Update:
    def __init__(self, query):
        self.callback_query = query


class _Context:
    def __init__(self):
        self.bot = object()


def _run(coro):
    return asyncio.run(coro)


def test_retry_rejects_button_owned_by_another_user(monkeypatch):
    query = _Query(user_id=42, data="retry_errors_99")
    monkeypatch.setattr(
        retry,
        "load_retry_source_for_result_message",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not load source")),
    )

    result = _run(retry.retry_errors(_Update(query), _Context()))

    assert result == retry.ConversationHandler.END
    assert query.answers == [("Эта кнопка принадлежит другому пользователю.", True)]


def test_retry_starts_from_durable_source_and_sends_first_question(monkeypatch):
    question = {
        "id": "q1",
        "question": "First?",
        "options": ["A", "B"],
        "correct": "A",
    }
    source = RetrySource(
        session_id="source-session",
        level_name="Easy",
        questions=(question,),
    )
    lookup = {}
    launch = {}
    sent = []

    def load_source(**kwargs):
        lookup.update(kwargs)
        return source

    async def launch_attempt(**kwargs):
        launch.update(kwargs)
        return {"session_id": "retry-session"}

    async def send_question(bot, user_id):
        sent.append((bot, user_id))

    monkeypatch.setattr(retry, "load_retry_source_for_result_message", load_source)
    monkeypatch.setattr(retry.quiz, "_launch_attempt", launch_attempt)
    monkeypatch.setattr(retry.quiz, "send_question", send_question)

    query = _Query()
    context = _Context()
    result = _run(retry.retry_errors(_Update(query), context))

    assert result == retry.quiz.ANSWERING
    assert lookup == {
        "user_id": 42,
        "chat_id": 777,
        "message_date": query.message.date,
    }
    assert launch["user"] is query.from_user
    assert launch["bot"] is context.bot
    assert launch["chat_id"] == 777
    assert launch["mode"] == "level"
    assert launch["questions"] == [question]
    assert launch["level_key"] == "retry_errors"
    assert launch["level_name"] == "🔁 Повторение ошибок (Easy)"
    assert launch["time_limit"] is None
    assert launch["is_retry"] is True
    assert sent == [(context.bot, 42)]
    assert query.answers == [(None, False)]
    assert query.edits
    assert "Вопросов: 1" in query.edits[0][0]


def test_retry_fails_closed_when_mongo_source_is_unavailable(monkeypatch):
    def unavailable(**_kwargs):
        raise LegacyRetrySourceUnavailable("mongo down")

    monkeypatch.setattr(retry, "load_retry_source_for_result_message", unavailable)
    monkeypatch.setattr(
        retry.quiz,
        "_launch_attempt",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not launch")),
    )
    query = _Query()

    result = _run(retry.retry_errors(_Update(query), _Context()))

    assert result == retry.ConversationHandler.END
    assert query.answers == [("База результатов временно недоступна.", True)]


def test_retry_rejects_invalid_or_missing_durable_source(monkeypatch):
    query = _Query()

    monkeypatch.setattr(
        retry,
        "load_retry_source_for_result_message",
        lambda **_kwargs: None,
    )
    result = _run(retry.retry_errors(_Update(query), _Context()))
    assert result == retry.ConversationHandler.END
    assert query.answers[-1] == ("Данные результата устарели.", True)

    query = _Query()

    def invalid(**_kwargs):
        raise LegacyRetrySourceInvalid("bad ledger")

    monkeypatch.setattr(retry, "load_retry_source_for_result_message", invalid)
    result = _run(retry.retry_errors(_Update(query), _Context()))
    assert result == retry.ConversationHandler.END
    assert query.answers[-1] == ("Сохранённый результат повреждён.", True)


def test_retry_does_not_offer_practice_when_durable_source_has_no_errors(monkeypatch):
    monkeypatch.setattr(
        retry,
        "load_retry_source_for_result_message",
        lambda **_kwargs: RetrySource(
            session_id="source-session",
            level_name="Easy",
            questions=(),
        ),
    )
    query = _Query()

    result = _run(retry.retry_errors(_Update(query), _Context()))

    assert result == retry.ConversationHandler.END
    assert query.answers == [("Ошибок нет!", True)]


def test_retry_source_name_does_not_nest_practice_label():
    assert retry._display_source_name("Easy") == "Easy"
    assert (
        retry._display_source_name("🔁 Повторение ошибок (Easy)")
        == "Easy"
    )
