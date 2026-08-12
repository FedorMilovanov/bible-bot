import asyncio
import os
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest

os.environ.setdefault("ADMIN_USER_ID", "1")

import telegram_battle_create_controller as create
from legacy_battle_session import LegacyBattleSessionUnavailable


class Query:
    def __init__(self):
        self.from_user = SimpleNamespace(id=42, first_name="Creator")
        self.answers = []
        self.edits = []

    async def answer(self, text=None, show_alert=False):
        self.answers.append((text, show_alert))

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


class Bot:
    username = "BibleQuizBot"


def run(coro):
    return asyncio.run(coro)


def battle(battle_id, *, creator_id=42):
    return {
        "_id": battle_id,
        "creator_id": creator_id,
        "creator_name": "Creator",
        "status": "waiting",
    }


def test_update_id_maps_to_exact_stable_battle_id():
    assert create.battle_id_for_update(0) == "battle_0000000000000000"
    assert create.battle_id_for_update(77) == "battle_000000000000004d"
    assert create.battle_id_for_update(77) == create.battle_id_for_update(77)
    assert create.battle_id_for_update(78) != create.battle_id_for_update(77)
    for invalid in (True, -1, 1 << 64, "77"):
        with pytest.raises(ValueError):
            create.battle_id_for_update(invalid)


def test_same_update_replay_reuses_existing_battle_without_resampling(monkeypatch):
    battle_id = create.battle_id_for_update(77)
    existing = battle(battle_id)
    monkeypatch.setattr(
        create,
        "get_owned_open_durable_battle",
        lambda value, user_id: existing if (value, user_id) == (battle_id, 42) else None,
    )
    monkeypatch.setattr(
        create.battles,
        "_battle_pool",
        lambda: (_ for _ in ()).throw(AssertionError("replay must not resample questions")),
    )
    monkeypatch.setattr(
        create,
        "create_durable_battle",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("replay must not insert")),
    )
    update = SimpleNamespace(update_id=77)
    user = SimpleNamespace(id=42, first_name="Creator")

    recovered, created = create.create_or_recover_battle(update, user)

    assert recovered == existing
    assert created is False


def test_ambiguous_insert_error_recovers_exact_committed_battle(monkeypatch):
    battle_id = create.battle_id_for_update(88)
    committed = battle(battle_id)
    lookups = iter([None, committed])
    monkeypatch.setattr(
        create,
        "get_owned_open_durable_battle",
        lambda _battle_id, _user_id: next(lookups),
    )
    monkeypatch.setattr(create.battles, "_battle_pool", lambda: [{"id": "q1"}])
    monkeypatch.setattr(
        create,
        "create_durable_battle",
        lambda **_kwargs: (_ for _ in ()).throw(LegacyBattleSessionUnavailable("lost ack")),
    )
    update = SimpleNamespace(update_id=88)
    user = SimpleNamespace(id=42, first_name="Creator")

    recovered, created = create.create_or_recover_battle(update, user)

    assert recovered == committed
    assert created is False


def test_unproven_ambiguous_insert_fails_closed(monkeypatch):
    monkeypatch.setattr(create, "get_owned_open_durable_battle", lambda *_args: None)
    monkeypatch.setattr(create.battles, "_battle_pool", lambda: [{"id": "q1"}])
    monkeypatch.setattr(
        create,
        "create_durable_battle",
        lambda **_kwargs: (_ for _ in ()).throw(LegacyBattleSessionUnavailable("down")),
    )

    with pytest.raises(LegacyBattleSessionUnavailable, match="down"):
        create.create_or_recover_battle(
            SimpleNamespace(update_id=99),
            SimpleNamespace(id=42, first_name="Creator"),
        )


def test_existing_deterministic_id_must_be_creator_owned(monkeypatch):
    battle_id = create.battle_id_for_update(100)
    monkeypatch.setattr(
        create,
        "get_owned_open_durable_battle",
        lambda *_args: battle(battle_id, creator_id=7),
    )

    with pytest.raises(Exception, match="not owned"):
        create.create_or_recover_battle(
            SimpleNamespace(update_id=100),
            SimpleNamespace(id=42, first_name="Creator"),
        )


def test_rendered_share_link_uses_deterministic_created_id(monkeypatch):
    battle_id = create.battle_id_for_update(101)
    monkeypatch.setattr(
        create,
        "create_or_recover_battle",
        lambda _update, _user: (battle(battle_id), True),
    )
    query = Query()
    update = SimpleNamespace(update_id=101, callback_query=query)

    run(create.create_battle(update, SimpleNamespace(bot=Bot())))

    assert query.answers == [(None, False)]
    markup = query.edits[0][1]["reply_markup"]
    share_button = markup.inline_keyboard[0][0]
    params = parse_qs(urlparse(share_button.url).query)
    assert params["url"] == [
        f"https://t.me/BibleQuizBot?start=duel_{battle_id}"
    ]
    assert markup.inline_keyboard[1][0].callback_data == f"cancel_battle_{battle_id}"
