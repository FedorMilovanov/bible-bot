import asyncio
from types import SimpleNamespace

import telegram_settings_controller as settings


class Query:
    def __init__(self, *, user_id=7, data=""):
        self.from_user = SimpleNamespace(id=user_id)
        self.data = data
        self.answers = 0
        self.edits = []

    async def answer(self):
        self.answers += 1

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


def _update(query):
    return SimpleNamespace(callback_query=query)


def _button_callback(query):
    markup = query.edits[-1][1]["reply_markup"]
    return markup.inline_keyboard[0][0].callback_data


def test_settings_menu_emits_target_specific_callback(monkeypatch):
    monkeypatch.setattr(settings, "get_pref", lambda *_args, **_kwargs: True)
    query = Query(data="user_settings")

    asyncio.run(settings.user_settings_handler(_update(query), object()))

    assert query.answers == 1
    assert _button_callback(query) == "typewriter_set:0"


def test_explicit_set_callback_is_idempotent_on_replay(monkeypatch):
    state = {"value": True}
    writes = []

    def get_pref(*_args, **_kwargs):
        return state["value"]

    def set_pref(_user_id, _key, value):
        writes.append(value)
        state["value"] = value

    monkeypatch.setattr(settings, "get_pref", get_pref)
    monkeypatch.setattr(settings, "set_pref", set_pref)

    first = Query(data="typewriter_set:0")
    asyncio.run(settings.set_typewriter_handler(_update(first), object()))
    replay = Query(data="typewriter_set:0")
    asyncio.run(settings.set_typewriter_handler(_update(replay), object()))

    assert writes == [False, False]
    assert state["value"] is False
    assert _button_callback(replay) == "typewriter_set:1"


def test_old_toggle_callback_only_upgrades_menu_without_mutation(monkeypatch):
    monkeypatch.setattr(settings, "get_pref", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        settings,
        "set_pref",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("compatibility callback must not toggle")
        ),
    )
    query = Query(data="toggle_typewriter")

    asyncio.run(settings.legacy_toggle_upgrade_handler(_update(query), object()))

    assert query.answers == 1
    assert _button_callback(query) == "typewriter_set:0"


def test_invalid_set_payload_does_not_write(monkeypatch):
    monkeypatch.setattr(
        settings,
        "set_pref",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("invalid payload must not write")
        ),
    )
    query = Query(data="typewriter_set:maybe")

    asyncio.run(settings.set_typewriter_handler(_update(query), object()))

    assert query.answers == 1
    assert query.edits == []


def test_preferences_are_owned_process_locally_without_bot_import():
    settings._USER_PREFS.clear()
    assert settings.get_pref(7, "typewriter") is True
    settings.set_pref(7, "typewriter", False)
    assert settings.get_pref(7, "typewriter") is False

    source = __import__("pathlib").Path(settings.__file__).read_text(encoding="utf-8")
    assert "import bot" not in source
    assert "from bot" not in source
