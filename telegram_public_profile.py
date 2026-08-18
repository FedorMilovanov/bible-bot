"""Canonical public Telegram bot profile and idempotent provider sync."""
from __future__ import annotations

from telegram.constants import BotDescriptionLimit, BotNameLimit

PUBLIC_BOT_NAME = "Библейский тренажёр — 1 Петра"
PUBLIC_BOT_SHORT_DESCRIPTION = (
    "Библейский тест по 1 Петра: главы, таймер, Challenge 20, повтор ошибок, "
    "достижения и история."
)
PUBLIC_BOT_DESCRIPTION = (
    "Библейский тренажёр по Первому посланию Петра. Проходите главы в спокойном, "
    "таймерном и скоростном режимах, повторяйте ошибки, запускайте Challenge 20 и "
    "следите за достижениями и историей результатов. Mini App работает прямо в "
    "Telegram. Материалы и исследования: gospod-bog.ru/app/"
)

# Keep one canonical Russian product identity both as the default fallback and
# for Telegram clients that have a dedicated Russian provider value.
PUBLIC_PROFILE_LANGUAGE_CODES = ("", "ru")


def _validate_profile_contract() -> None:
    if not PUBLIC_BOT_NAME or len(PUBLIC_BOT_NAME) > int(BotNameLimit.MAX_NAME_LENGTH):
        raise RuntimeError("canonical Telegram bot name violates provider limits")
    if len(PUBLIC_BOT_SHORT_DESCRIPTION) > int(
        BotDescriptionLimit.MAX_SHORT_DESCRIPTION_LENGTH
    ):
        raise RuntimeError("canonical Telegram bot short description violates provider limits")
    if len(PUBLIC_BOT_DESCRIPTION) > int(BotDescriptionLimit.MAX_DESCRIPTION_LENGTH):
        raise RuntimeError("canonical Telegram bot description violates provider limits")


_validate_profile_contract()


async def sync_public_profile(bot) -> dict[str, object]:
    """Read/compare/write the public Telegram profile for default and Russian locales.

    The function is intentionally idempotent: every field is read first and only
    provider values that differ from the canonical contract are written. Provider
    exceptions, including RetryAfter, propagate to the caller so the production
    startup retry policy remains the single retry authority.
    """

    writes: list[str] = []
    reads = 0
    for language_code in PUBLIC_PROFILE_LANGUAGE_CODES:
        locale = language_code or "default"

        current_name = await bot.get_my_name(language_code=language_code)
        reads += 1
        if current_name.name != PUBLIC_BOT_NAME:
            await bot.set_my_name(name=PUBLIC_BOT_NAME, language_code=language_code)
            writes.append(f"{locale}:name")

        current_short = await bot.get_my_short_description(language_code=language_code)
        reads += 1
        if current_short.short_description != PUBLIC_BOT_SHORT_DESCRIPTION:
            await bot.set_my_short_description(
                short_description=PUBLIC_BOT_SHORT_DESCRIPTION,
                language_code=language_code,
            )
            writes.append(f"{locale}:short_description")

        current_description = await bot.get_my_description(language_code=language_code)
        reads += 1
        if current_description.description != PUBLIC_BOT_DESCRIPTION:
            await bot.set_my_description(
                description=PUBLIC_BOT_DESCRIPTION,
                language_code=language_code,
            )
            writes.append(f"{locale}:description")

    return {"reads": reads, "writes": tuple(writes)}
