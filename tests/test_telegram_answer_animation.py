import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import telegram_answer_animation as animation


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_answer_animation.py").read_text(encoding="utf-8")


class _Query:
    def __init__(self, markup, *, fail_edit=False):
        self.message = SimpleNamespace(reply_markup=markup)
        self.fail_edit = fail_edit
        self.edits = []

    async def edit_message_reply_markup(self, *, reply_markup):
        if self.fail_edit:
            raise RuntimeError("edit failed")
        self.edits.append(
            [
                [(button.text, button.callback_data) for button in row]
                for row in reply_markup.inline_keyboard
            ]
        )


def _run(coro):
    return asyncio.run(coro)


def _numeric_markup():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1", callback_data="qa_0"),
            InlineKeyboardButton("2", callback_data="qa_1"),
            InlineKeyboardButton("3", callback_data="qa_2"),
        ],
        [
            InlineKeyboardButton("flag", callback_data="report_0"),
            InlineKeyboardButton("exit", callback_data="cancel_quiz"),
        ],
    ])


def _text_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("alpha", callback_data="qa_0")],
        [InlineKeyboardButton("beta", callback_data="qa_1")],
        [InlineKeyboardButton("gamma", callback_data="qa_2")],
        [
            InlineKeyboardButton("flag", callback_data="report_0"),
            InlineKeyboardButton("exit", callback_data="cancel_quiz"),
        ],
    ])


def test_numeric_correct_answer_preserves_four_frame_animation(monkeypatch):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(animation.asyncio, "sleep", fake_sleep)
    original_markup = _numeric_markup()
    query = _Query(original_markup)

    _run(animation.animate_answer_buttons(query, 1, 1, True, ["alpha", "beta", "gamma"]))

    assert len(query.edits) == 4
    assert [frame[0][1][0] for frame in query.edits] == ["✅", "🎉", "⭐", "✅"]
    assert sleeps == [0.3, 0.3, 0.3, 0.3]
    for frame in query.edits:
        assert [callback for _, callback in frame[0]] == ["qa_0", "qa_1", "qa_2"]
        assert frame[-1] == [
            ("flag", "report_0"),
            ("exit", "cancel_quiz"),
        ]
    assert [button.text for button in original_markup.inline_keyboard[0]] == ["1", "2", "3"]


def test_numeric_wrong_answer_marks_clicked_and_correct_once(monkeypatch):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(animation.asyncio, "sleep", fake_sleep)
    query = _Query(_numeric_markup())

    _run(animation.animate_answer_buttons(query, 0, 2, True, ["alpha", "beta", "gamma"]))

    assert len(query.edits) == 1
    assert [text for text, _ in query.edits[0][0]] == ["❌", "2", "✅"]
    assert sleeps == []
    assert query.edits[0][-1] == [
        ("flag", "report_0"),
        ("exit", "cancel_quiz"),
    ]


def test_text_correct_answer_preserves_four_frame_animation(monkeypatch):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(animation.asyncio, "sleep", fake_sleep)
    query = _Query(_text_markup())

    _run(animation.animate_answer_buttons(query, 1, 1, False, ["alpha", "beta", "gamma"]))

    assert len(query.edits) == 4
    assert [frame[1][0][0] for frame in query.edits] == [
        "✅ beta",
        "🎉 beta",
        "⭐ beta",
        "✅ beta",
    ]
    assert sleeps == [0.3, 0.3, 0.3, 0.3]
    for frame in query.edits:
        assert frame[0][0][1] == "qa_0"
        assert frame[1][0][1] == "qa_1"
        assert frame[2][0][1] == "qa_2"
        assert frame[-1] == [
            ("flag", "report_0"),
            ("exit", "cancel_quiz"),
        ]


def test_text_wrong_answer_marks_clicked_and_correct_once(monkeypatch):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(animation.asyncio, "sleep", fake_sleep)
    query = _Query(_text_markup())

    _run(animation.animate_answer_buttons(query, 0, 2, False, ["alpha", "beta", "gamma"]))

    assert len(query.edits) == 1
    assert query.edits[0][0][0][0] == "❌ alpha"
    assert query.edits[0][1][0][0] == "beta"
    assert query.edits[0][2][0][0] == "✅ gamma"
    assert sleeps == []


def test_edit_failure_and_malformed_markup_remain_fail_soft(monkeypatch):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(animation.asyncio, "sleep", fake_sleep)
    query = _Query(_numeric_markup(), fail_edit=True)
    _run(animation.animate_answer_buttons(query, 1, 1, True, ["alpha", "beta", "gamma"]))
    assert sleeps == []

    malformed = _Query(None)
    _run(animation.animate_answer_buttons(malformed, 0, 0, True, ["alpha"]))
    assert malformed.edits == []


def test_bridge_replaces_only_expected_legacy_animation_callable():
    async def old_animation(query, btn_index, correct_index, is_numeric_mode, shuffled):
        return None

    legacy = SimpleNamespace(_animate_answer_buttons=old_animation, marker=object())
    marker = legacy.marker

    animation.install_legacy_bridge(legacy)

    assert legacy._animate_answer_buttons is animation.animate_answer_buttons
    assert legacy.marker is marker


def test_bridge_rejects_missing_or_non_callable_legacy_animation():
    with pytest.raises(TypeError):
        animation.validate_legacy_bridge(SimpleNamespace())
    with pytest.raises(TypeError):
        animation.validate_legacy_bridge(SimpleNamespace(_animate_answer_buttons=None))


def test_focused_animation_has_no_legacy_or_durable_state_dependency():
    assert "import bot" not in SOURCE
    assert "from bot" not in SOURCE
    assert "user_data" not in SOURCE
    assert "storage" not in SOURCE
    assert ".text =" not in SOURCE
