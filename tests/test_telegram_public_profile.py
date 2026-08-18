import asyncio
from types import SimpleNamespace

import pytest
from telegram.constants import BotDescriptionLimit, BotNameLimit

import telegram_public_profile as profile


def run(coro):
    return asyncio.run(coro)


class Bot:
    def __init__(self, values=None, *, read_error=None, write_error=None):
        self.values = values or {}
        self.read_error = read_error
        self.write_error = write_error
        self.read_calls = []
        self.write_calls = []

    def _value(self, language_code, field):
        return self.values.get((language_code, field), "")

    async def _read(self, language_code, field):
        self.read_calls.append((language_code, field))
        if self.read_error is not None:
            raise self.read_error
        return self._value(language_code, field)

    async def _write(self, language_code, field, value):
        self.write_calls.append((language_code, field, value))
        if self.write_error is not None:
            raise self.write_error
        self.values[(language_code, field)] = value
        return True

    async def get_my_name(self, *, language_code):
        return SimpleNamespace(name=await self._read(language_code, "name"))

    async def set_my_name(self, *, name, language_code):
        return await self._write(language_code, "name", name)

    async def get_my_short_description(self, *, language_code):
        return SimpleNamespace(
            short_description=await self._read(language_code, "short_description")
        )

    async def set_my_short_description(self, *, short_description, language_code):
        return await self._write(
            language_code, "short_description", short_description
        )

    async def get_my_description(self, *, language_code):
        return SimpleNamespace(
            description=await self._read(language_code, "description")
        )

    async def set_my_description(self, *, description, language_code):
        return await self._write(language_code, "description", description)


def canonical_values():
    values = {}
    for language_code in profile.PUBLIC_PROFILE_LANGUAGE_CODES:
        values[(language_code, "name")] = profile.PUBLIC_BOT_NAME
        values[(language_code, "short_description")] = (
            profile.PUBLIC_BOT_SHORT_DESCRIPTION
        )
        values[(language_code, "description")] = profile.PUBLIC_BOT_DESCRIPTION
    return values


def test_canonical_profile_fits_telegram_provider_limits():
    assert 0 < len(profile.PUBLIC_BOT_NAME) <= int(BotNameLimit.MAX_NAME_LENGTH)
    assert len(profile.PUBLIC_BOT_SHORT_DESCRIPTION) <= int(
        BotDescriptionLimit.MAX_SHORT_DESCRIPTION_LENGTH
    )
    assert len(profile.PUBLIC_BOT_DESCRIPTION) <= int(
        BotDescriptionLimit.MAX_DESCRIPTION_LENGTH
    )
    assert profile.PUBLIC_PROFILE_LANGUAGE_CODES == ("", "ru")


def test_already_correct_profile_is_read_but_never_rewritten():
    bot = Bot(canonical_values())

    result = run(profile.sync_public_profile(bot))

    assert result == {"reads": 6, "writes": ()}
    assert len(bot.read_calls) == 6
    assert bot.write_calls == []


def test_only_mismatched_provider_field_is_written():
    values = canonical_values()
    values[("ru", "short_description")] = "старое описание"
    bot = Bot(values)

    result = run(profile.sync_public_profile(bot))

    assert result == {"reads": 6, "writes": ("ru:short_description",)}
    assert bot.write_calls == [
        ("ru", "short_description", profile.PUBLIC_BOT_SHORT_DESCRIPTION)
    ]


def test_empty_provider_profile_is_reconciled_for_default_and_russian():
    bot = Bot()

    result = run(profile.sync_public_profile(bot))

    assert result["reads"] == 6
    assert result["writes"] == (
        "default:name",
        "default:short_description",
        "default:description",
        "ru:name",
        "ru:short_description",
        "ru:description",
    )
    assert bot.values == canonical_values()


def test_provider_read_failure_propagates_to_startup_retry_owner():
    error = RuntimeError("provider read failed")
    bot = Bot(read_error=error)

    with pytest.raises(RuntimeError, match="provider read failed"):
        run(profile.sync_public_profile(bot))
    assert bot.write_calls == []


def test_provider_write_failure_is_not_hidden():
    error = RuntimeError("provider write failed")
    bot = Bot(write_error=error)

    with pytest.raises(RuntimeError, match="provider write failed"):
        run(profile.sync_public_profile(bot))
