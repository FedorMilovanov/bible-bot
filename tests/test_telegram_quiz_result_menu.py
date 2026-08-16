import asyncio
from pathlib import Path
from types import SimpleNamespace

import telegram_quiz_result_menu as result_menu


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_quiz_result_menu.py").read_text(encoding="utf-8")


class _Bot:
    def __init__(self, *, username="BibleBot", fail_get_me=False, fail_send=False):
        self.username = username
        self.fail_get_me = fail_get_me
        self.fail_send = fail_send
        self.sent = []

    async def get_me(self):
        if self.fail_get_me:
            raise RuntimeError("get_me failed")
        return SimpleNamespace(username=self.username)

    async def send_message(self, **kwargs):
        if self.fail_send:
            raise RuntimeError("send failed")
        self.sent.append(kwargs)


def _run(coro):
    return asyncio.run(coro)


def _answer(user_answer, *, correct="beta"):
    return {
        "user_answer": user_answer,
        "question_obj": {
            "question": "probe",
            "options": ["alpha", correct],
            "correct": 1,
        },
    }


def _share_button(markup):
    for row in markup.inline_keyboard:
        for button in row:
            if button.switch_inline_query is not None:
                return button
    raise AssertionError("share button not found")


def test_perfect_result_preserves_callbacks_and_share_projection():
    bot = _Bot(username="BibleBot")
    data = {
        "questions": [{} for _ in range(10)],
        "correct_answers": 10,
        "level_key": "chapter1",
        "level_name": "Level One",
        "user_id": 42,
        "max_streak": 4,
    }

    _run(result_menu.send_final_results_menu(bot, 42, data))

    assert len(bot.sent) == 1
    message = bot.sent[0]
    assert message["chat_id"] == 42
    assert "10/10" in message["text"]
    assert "100%" in message["text"]
    assert message["parse_mode"] == "Markdown"

    keyboard = message["reply_markup"].inline_keyboard
    assert keyboard[0][0].callback_data == "review_test_0"
    assert all(
        button.callback_data != "retry_errors_42"
        for row in keyboard
        for button in row
    )
    assert any(
        button.callback_data == "level_chapter1"
        for row in keyboard
        for button in row
    )
    assert keyboard[-1][0].callback_data == "start_test"
    assert keyboard[-1][1].callback_data == "back_to_main"

    share = _share_button(message["reply_markup"]).switch_inline_query
    assert "🟩" * 10 in share
    assert "100%" in share
    assert "10/10" in share
    assert "🔥" in share
    assert "@BibleBot" in share


def test_missing_correct_count_uses_canonical_answer_history_and_fallback_username():
    bot = _Bot(fail_get_me=True)
    data = {
        "questions": [{}, {}],
        "answered_questions": [
            _answer("beta"),
            _answer("alpha"),
        ],
        "user_id": 77,
        "level_name": "Fallback",
    }

    _run(result_menu.send_final_results_menu(bot, 77, data))

    message = bot.sent[0]
    assert "1/2" in message["text"]
    assert "50%" in message["text"]
    keyboard = message["reply_markup"].inline_keyboard
    assert any(
        button.callback_data == "retry_errors_77"
        for row in keyboard
        for button in row
    )
    assert not any(
        button.callback_data and button.callback_data.startswith("level_")
        for row in keyboard
        for button in row
    )
    share = _share_button(message["reply_markup"]).switch_inline_query
    assert "@milovanovaibot" in share


def test_challenge_share_preserves_mode_progress_and_bonus():
    bot = _Bot(username="ChallengeBot")
    data = {
        "questions": [{} for _ in range(5)],
        "correct_answers": 4,
        "user_id": 7,
        "level_name": "Challenge",
        "challenge_mode": "random20",
        "challenge_bonus": 15,
    }

    _run(result_menu.send_final_results_menu(bot, 7, data))

    share = _share_button(bot.sent[0]["reply_markup"]).switch_inline_query
    assert "🎲 Random Challenge" in share
    assert "🟩" * 8 + "⬜" * 2 in share
    assert "80%" in share
    assert "4/5" in share
    assert "+15" in share
    assert "@ChallengeBot" in share


def test_send_failure_is_logged_and_swallowed():
    bot = _Bot(fail_send=True)
    data = {"questions": [{}], "correct_answers": 1}

    _run(result_menu.send_final_results_menu(bot, 1, data))


def test_focused_result_menu_has_no_retired_monolith_bridge():
    assert "import bot" not in SOURCE
    assert "from bot" not in SOURCE
    assert "install_legacy_bridge" not in SOURCE
    assert "validate_legacy_bridge" not in SOURCE
