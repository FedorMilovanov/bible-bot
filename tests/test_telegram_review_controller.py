import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import telegram_review_controller as review


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def _question():
    return {
        "question": "Кто написал послание?",
        "options": ["Пётр", "Павел", "Иоанн"],
        "correct": 0,
        "explanation": "Автор называет себя Петром.",
        "verse": "1:1",
        "topic": "Авторство",
    }


def test_review_test_reads_only_supplied_runtime_map():
    query = SimpleNamespace(
        data="review_test_0",
        from_user=SimpleNamespace(id=7),
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    runtime = {
        7: {
            "answered_questions": [
                {"question_obj": _question(), "user_answer": "Павел"}
            ]
        }
    }

    asyncio.run(
        review.review_test_handler(
            SimpleNamespace(callback_query=query),
            SimpleNamespace(),
            user_data=runtime,
        )
    )

    query.answer.assert_awaited_once_with()
    kwargs = query.edit_message_text.await_args.kwargs
    assert "📖 *Просмотр теста* (1/1)" in kwargs["text"]
    assert "✅ 1. Пётр" in kwargs["text"]
    assert "❌ 2. Павел ← твой ответ" in kwargs["text"]
    assert kwargs["parse_mode"] == "Markdown"
    payloads = [
        button.callback_data
        for row in kwargs["reply_markup"].inline_keyboard
        for button in row
    ]
    assert payloads == ["noop", "back_to_main"]


def test_review_errors_rejects_foreign_user():
    query = SimpleNamespace(
        data="review_errors_8_0",
        from_user=SimpleNamespace(id=7),
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )

    asyncio.run(
        review.review_errors_handler(
            SimpleNamespace(callback_query=query),
            SimpleNamespace(),
            user_data={},
        )
    )

    query.answer.assert_awaited_once_with(
        "Нет доступа к чужому разбору ошибок.",
        show_alert=True,
    )
    query.edit_message_text.assert_not_awaited()


def test_review_errors_preserves_navigation_and_clamps_index():
    wrong = [
        {"question_obj": _question(), "user_answer": "Павел"},
        {"question_obj": _question(), "user_answer": "Иоанн"},
    ]
    query = SimpleNamespace(
        data="review_nav_99",
        from_user=SimpleNamespace(id=7),
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )

    asyncio.run(
        review.review_errors_handler(
            SimpleNamespace(callback_query=query),
            SimpleNamespace(),
            user_data={7: {"wrong_answers": wrong}},
        )
    )

    kwargs = query.edit_message_text.await_args.kwargs
    assert "Ошибка 2 из 2" in query.edit_message_text.await_args.args[0]
    markup = kwargs["reply_markup"]
    first_row = [button.callback_data for button in markup.inline_keyboard[0]]
    assert first_row == ["review_nav_0", "review_nav_noop", "review_nav_noop"]
    assert markup.inline_keyboard[1][0].callback_data == "back_to_main"


def test_review_nav_noop_only_acknowledges():
    query = SimpleNamespace(
        data="review_nav_noop",
        from_user=SimpleNamespace(id=7),
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )

    asyncio.run(
        review.review_errors_handler(
            SimpleNamespace(callback_query=query),
            SimpleNamespace(),
            user_data={},
        )
    )

    query.answer.assert_awaited_once_with()
    query.edit_message_text.assert_not_awaited()


def test_production_routes_review_handlers_outside_legacy():
    assert "import telegram_review_controller as review" in PRODUCTION_SOURCE
    assert "async def _review_errors_handler" in PRODUCTION_SOURCE
    assert "async def _review_test_handler" in PRODUCTION_SOURCE
    assert "user_data=quiz.user_data" in PRODUCTION_SOURCE
    assert "CallbackQueryHandler(_review_errors_handler, pattern=r\"^review_errors_\")" in PRODUCTION_SOURCE
    assert "CallbackQueryHandler(_review_errors_handler, pattern=r\"^review_nav_\")" in PRODUCTION_SOURCE
    assert "CallbackQueryHandler(_review_test_handler, pattern=r\"^review_test_\\d+$\")" in PRODUCTION_SOURCE
    assert "legacy.review_errors_handler" not in PRODUCTION_SOURCE
    assert "legacy.review_test_handler" not in PRODUCTION_SOURCE
