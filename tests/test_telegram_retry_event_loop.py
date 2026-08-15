import asyncio
import os
import threading
from datetime import UTC, datetime
from types import SimpleNamespace

os.environ.setdefault("ADMIN_USER_ID", "1")

import telegram_retry_controller as retry
from legacy_retry_source import RetrySource


def test_retry_source_lookup_runs_off_ptb_event_loop_thread(monkeypatch):
    event_loop_thread = threading.get_ident()
    worker_threads = []
    query = SimpleNamespace(
        from_user=SimpleNamespace(
            id=42,
            username="tester",
            first_name="Tester",
        ),
        data="retry_errors_42",
        message=SimpleNamespace(
            chat_id=777,
            date=datetime(2026, 8, 15, 16, 0, tzinfo=UTC),
        ),
        answers=[],
        edits=[],
    )

    async def answer(text=None, show_alert=False):
        query.answers.append((text, show_alert))

    async def edit_message_text(text, **kwargs):
        query.edits.append((text, kwargs))

    query.answer = answer
    query.edit_message_text = edit_message_text

    def load_source(**kwargs):
        worker_threads.append(threading.get_ident())
        assert kwargs == {
            "user_id": 42,
            "chat_id": 777,
            "message_date": query.message.date,
        }
        return RetrySource(
            session_id="source-session",
            level_name="Easy",
            questions=(
                {
                    "id": "q1",
                    "question": "First?",
                    "options": ["A", "B"],
                    "correct": "A",
                },
            ),
        )

    async def launch_attempt(**_kwargs):
        return {"session_id": "retry-session"}

    async def send_question(_bot, _user_id):
        return None

    monkeypatch.setattr(retry, "load_retry_source_for_result_message", load_source)
    monkeypatch.setattr(retry.quiz, "_launch_attempt", launch_attempt)
    monkeypatch.setattr(retry.quiz, "send_question", send_question)

    update = SimpleNamespace(callback_query=query)
    context = SimpleNamespace(bot=object())

    result = asyncio.run(retry.retry_errors(update, context))

    assert result == retry.quiz.ANSWERING
    assert len(worker_threads) == 1
    assert worker_threads[0] != event_loop_thread
    assert query.answers == [(None, False)]
