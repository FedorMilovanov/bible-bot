import asyncio
import os
import threading
from datetime import UTC, datetime
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest

os.environ.setdefault("ADMIN_USER_ID", "1")

import telegram_battle_share_controller as share
from legacy_battle_session import LegacyBattleSessionUnavailable


class _Message:
    def __init__(self):
        self.chat_id = 777
        self.date = datetime(2026, 8, 11, 18, 0, tzinfo=UTC)
        self.replies = []

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class _Bot:
    def __init__(self, username="BibleQuizBot"):
        self.username = username
        self.sent = []

    async def send_message(self, **kwargs):
        self.sent.append(kwargs)


class _Query:
    def __init__(self):
        self.from_user = SimpleNamespace(id=42, first_name="Creator")
        self.answers = []
        self.edits = []

    async def answer(self, text=None, show_alert=False):
        self.answers.append((text, show_alert))

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


def _run(coro):
    return asyncio.run(coro)


def _battle(battle_id="battle_0123456789abcdef"):
    return {
        "_id": battle_id,
        "creator_id": 42,
        "creator_name": "Creator",
        "opponent_id": 99,
        "opponent_name": "Opponent",
        "status": "in_progress",
    }


def test_share_url_contains_exact_durable_deep_link():
    battle_id = "battle_0123456789abcdef"
    deep_link = share.build_battle_deep_link("@BibleQuizBot", battle_id)
    assert deep_link == (
        "https://t.me/BibleQuizBot?start=duel_battle_0123456789abcdef"
    )

    share_url = share.build_battle_share_url("BibleQuizBot", battle_id, "Creator")
    parsed = urlparse(share_url)
    params = parse_qs(parsed.query)
    assert (parsed.scheme, parsed.netloc, parsed.path) == (
        "https",
        "t.me",
        "/share/url",
    )
    assert params["url"] == [deep_link]
    assert "Creator" in params["text"][0]


def test_deep_link_parser_is_exact_and_leaves_normal_start_alone():
    battle_id = "battle_0123456789abcdef"
    assert share.parse_battle_deep_link([]) is None
    assert share.parse_battle_deep_link(["anything_else"]) is None
    assert share.parse_battle_deep_link([f"duel_{battle_id}"]) == battle_id

    with pytest.raises(ValueError, match="malformed"):
        share.parse_battle_deep_link(["duel_battle_nothex"])
    with pytest.raises(ValueError, match="unexpected"):
        share.parse_battle_deep_link([f"duel_{battle_id}", "extra"])


def test_handle_start_deep_link_returns_false_for_ordinary_start(monkeypatch):
    monkeypatch.setattr(
        share,
        "claim_durable_battle_opponent",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not claim")),
    )
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=99, first_name="Opponent"),
        effective_message=_Message(),
    )
    context = SimpleNamespace(args=[], bot=_Bot())

    assert _run(share.handle_start_deep_link(update, context)) is False
    assert update.effective_message.replies == []


def test_handle_start_deep_link_claims_exact_battle_and_routes_creator_notice(monkeypatch):
    battle_id = "battle_0123456789abcdef"
    claims = []
    ready_calls = []

    def claim(*args, **kwargs):
        claims.append((args, kwargs))
        return _battle(battle_id)

    async def deliver(bot, battle_id_arg, *, start_payload_builder):
        ready_calls.append((bot, battle_id_arg, start_payload_builder))
        return True

    monkeypatch.setattr(share, "claim_durable_battle_opponent", claim)
    monkeypatch.setattr(share.ready_delivery, "deliver_creator_ready_once", deliver)
    message = _Message()
    bot = _Bot()
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=99, first_name="Opponent"),
        effective_message=message,
    )
    context = SimpleNamespace(args=[f"duel_{battle_id}"], bot=bot)

    assert _run(share.handle_start_deep_link(update, context)) is True
    assert claims == [((battle_id, 99, "Opponent"), {})]
    assert len(message.replies) == 1
    assert "БИТВА НАЧАЛАСЬ" in message.replies[0][0]
    reply_markup = message.replies[0][1]["reply_markup"]
    assert reply_markup.inline_keyboard[0][0].callback_data == (
        f"start_battle_{battle_id}_opponent"
    )
    assert ready_calls == [(bot, battle_id, share.battles._start_payload)]
    assert bot.sent == []


def test_deep_link_claim_runs_off_event_loop_thread(monkeypatch):
    battle_id = "battle_0123456789abcdef"
    event_loop_thread = threading.get_ident()
    worker_threads = []

    def claim(*args, **kwargs):
        assert args == (battle_id, 99, "Opponent")
        assert kwargs == {}
        worker_threads.append(threading.get_ident())
        return _battle(battle_id)

    async def deliver(*_args, **_kwargs):
        return True

    monkeypatch.setattr(share, "claim_durable_battle_opponent", claim)
    monkeypatch.setattr(share.ready_delivery, "deliver_creator_ready_once", deliver)
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=99, first_name="Opponent"),
        effective_message=_Message(),
    )
    context = SimpleNamespace(args=[f"duel_{battle_id}"], bot=_Bot())

    assert _run(share.handle_start_deep_link(update, context)) is True
    assert len(worker_threads) == 1
    assert worker_threads[0] != event_loop_thread


def test_handle_start_deep_link_fails_closed_for_stale_or_unavailable_battle(monkeypatch):
    battle_id = "battle_0123456789abcdef"
    message = _Message()
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=99, first_name="Opponent"),
        effective_message=message,
    )
    context = SimpleNamespace(args=[f"duel_{battle_id}"], bot=_Bot())

    monkeypatch.setattr(share, "claim_durable_battle_opponent", lambda *_args: None)
    assert _run(share.handle_start_deep_link(update, context)) is True
    assert "уже занята" in message.replies[-1][0]

    def unavailable(*_args):
        raise LegacyBattleSessionUnavailable("mongo down")

    monkeypatch.setattr(share, "claim_durable_battle_opponent", unavailable)
    assert _run(share.handle_start_deep_link(update, context)) is True
    assert "База битв" in message.replies[-1][0]


def test_malformed_duel_payload_is_consumed_without_claim(monkeypatch):
    monkeypatch.setattr(
        share,
        "claim_durable_battle_opponent",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not claim")),
    )
    message = _Message()
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=99, first_name="Opponent"),
        effective_message=message,
    )
    context = SimpleNamespace(args=["duel_battle_bad"], bot=_Bot())

    assert _run(share.handle_start_deep_link(update, context)) is True
    assert "повреждена" in message.replies[-1][0]


def test_create_battle_exposes_share_picker_for_exact_created_id(monkeypatch):
    question = {
        "id": "q1",
        "question": "Question?",
        "options": ["A", "B"],
        "correct": "A",
    }
    created = {}

    monkeypatch.setattr(share.battles, "_battle_pool", lambda: [question])

    def create(**kwargs):
        created.update(kwargs)
        return dict(kwargs)

    monkeypatch.setattr(share, "create_durable_battle", create)
    query = _Query()
    context = SimpleNamespace(bot=_Bot())
    update = SimpleNamespace(callback_query=query)

    _run(share.create_battle(update, context))

    battle_id = created["battle_id"]
    assert battle_id.startswith("battle_")
    assert created["creator_id"] == 42
    assert created["questions"] == [question]
    assert query.answers == [(None, False)]
    markup = query.edits[0][1]["reply_markup"]
    share_button = markup.inline_keyboard[0][0]
    params = parse_qs(urlparse(share_button.url).query)
    assert params["url"] == [share.build_battle_deep_link("BibleQuizBot", battle_id)]
    assert markup.inline_keyboard[1][0].callback_data == f"cancel_battle_{battle_id}"


def test_create_battle_mongo_write_runs_off_event_loop_thread(monkeypatch):
    event_loop_thread = threading.get_ident()
    worker_threads = []
    question = {
        "id": "q1",
        "question": "Question?",
        "options": ["A", "B"],
        "correct": "A",
    }
    monkeypatch.setattr(share.battles, "_battle_pool", lambda: [question])

    def create(**kwargs):
        assert kwargs["creator_id"] == 42
        worker_threads.append(threading.get_ident())
        return dict(kwargs)

    monkeypatch.setattr(share, "create_durable_battle", create)
    query = _Query()
    update = SimpleNamespace(callback_query=query)

    _run(share.create_battle(update, SimpleNamespace(bot=_Bot())))

    assert len(worker_threads) == 1
    assert worker_threads[0] != event_loop_thread
    assert query.answers == [(None, False)]


def test_create_battle_still_works_when_bot_username_is_unavailable(monkeypatch):
    question = {
        "id": "q1",
        "question": "Question?",
        "options": ["A", "B"],
        "correct": "A",
    }
    monkeypatch.setattr(share.battles, "_battle_pool", lambda: [question])
    monkeypatch.setattr(share, "create_durable_battle", lambda **kwargs: dict(kwargs))
    query = _Query()
    update = SimpleNamespace(callback_query=query)

    _run(share.create_battle(update, SimpleNamespace(bot=_Bot(username=None))))

    markup = query.edits[0][1]["reply_markup"]
    assert all(button.url is None for row in markup.inline_keyboard for button in row)
