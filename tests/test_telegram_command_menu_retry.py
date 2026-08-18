import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest
from telegram import MenuButtonCommands, MenuButtonWebApp, WebAppInfo
from telegram.error import RetryAfter

import telegram_command_menu_retry as retry
import telegram_main_menu as main_menu
import telegram_public_profile as public_profile


def run(coro):
    return asyncio.run(coro)


def canonical_profile_values():
    values = {}
    for language_code in public_profile.PUBLIC_PROFILE_LANGUAGE_CODES:
        values[(language_code, "name")] = public_profile.PUBLIC_BOT_NAME
        values[(language_code, "short_description")] = (
            public_profile.PUBLIC_BOT_SHORT_DESCRIPTION
        )
        values[(language_code, "description")] = public_profile.PUBLIC_BOT_DESCRIPTION
    return values


class Bot:
    def __init__(
        self,
        *,
        command_error=None,
        current_menu_button=None,
        menu_read_error=None,
        menu_write_error=None,
        profile_values=None,
        profile_read_error=None,
        profile_write_error=None,
        has_main_web_app=True,
        username="testbot",
    ):
        self.command_error = command_error
        self.current_menu_button = current_menu_button or MenuButtonCommands()
        self.menu_read_error = menu_read_error
        self.menu_write_error = menu_write_error
        self.profile_values = (
            canonical_profile_values() if profile_values is None else profile_values
        )
        self.profile_read_error = profile_read_error
        self.profile_write_error = profile_write_error
        self.command_calls = []
        self.menu_read_calls = 0
        self.menu_write_calls = []
        self.profile_read_calls = []
        self.profile_write_calls = []
        self.bot = SimpleNamespace(
            username=username,
            has_main_web_app=has_main_web_app,
        )

    async def set_my_commands(self, commands):
        self.command_calls.append(commands)
        if self.command_error is not None:
            raise self.command_error
        return True

    async def _profile_read(self, language_code, field):
        self.profile_read_calls.append((language_code, field))
        if self.profile_read_error is not None:
            raise self.profile_read_error
        return self.profile_values.get((language_code, field), "")

    async def _profile_write(self, language_code, field, value):
        self.profile_write_calls.append((language_code, field, value))
        if self.profile_write_error is not None:
            raise self.profile_write_error
        self.profile_values[(language_code, field)] = value
        return True

    async def get_my_name(self, *, language_code):
        return SimpleNamespace(name=await self._profile_read(language_code, "name"))

    async def set_my_name(self, *, name, language_code):
        return await self._profile_write(language_code, "name", name)

    async def get_my_short_description(self, *, language_code):
        return SimpleNamespace(
            short_description=await self._profile_read(
                language_code, "short_description"
            )
        )

    async def set_my_short_description(self, *, short_description, language_code):
        return await self._profile_write(
            language_code, "short_description", short_description
        )

    async def get_my_description(self, *, language_code):
        return SimpleNamespace(
            description=await self._profile_read(language_code, "description")
        )

    async def set_my_description(self, *, description, language_code):
        return await self._profile_write(language_code, "description", description)

    async def get_chat_menu_button(self):
        self.menu_read_calls += 1
        if self.menu_read_error is not None:
            raise self.menu_read_error
        return self.current_menu_button

    async def set_chat_menu_button(self, *, menu_button):
        self.menu_write_calls.append(menu_button)
        if self.menu_write_error is not None:
            raise self.menu_write_error
        self.current_menu_button = menu_button
        return True


class JobQueue:
    def __init__(self):
        self.calls = []

    def run_once(self, callback, *, when):
        self.calls.append((callback, when))


async def retry_callback(context):
    del context


def setup_function():
    main_menu.configure_miniapp_url_provider(None)


def test_success_without_miniapp_does_not_touch_menu_button_or_schedule_retry():
    queue = JobQueue()
    context = SimpleNamespace(bot=Bot(), job_queue=queue)
    commands = ("menu", "test")

    assert run(retry.sync_public_commands_once(context, commands, retry_callback)) is True
    assert context.bot.command_calls == [commands]
    assert len(context.bot.profile_read_calls) == 6
    assert context.bot.profile_write_calls == []
    assert context.bot.menu_read_calls == 0
    assert context.bot.menu_write_calls == []
    assert queue.calls == []


def test_miniapp_menu_button_is_written_only_when_provider_state_differs():
    main_menu.configure_miniapp_url_provider(lambda: "https://example.test/app")
    queue = JobQueue()
    context = SimpleNamespace(bot=Bot(), job_queue=queue)

    assert run(retry.sync_public_commands_once(context, ("menu",), retry_callback)) is True

    assert context.bot.menu_read_calls == 1
    assert len(context.bot.menu_write_calls) == 1
    desired = context.bot.menu_write_calls[0]
    assert isinstance(desired, MenuButtonWebApp)
    assert desired.text == "🚀 Открыть приложение"
    assert desired.web_app.url == "https://example.test/app"
    assert queue.calls == []


def test_already_correct_miniapp_menu_button_is_not_rewritten():
    main_menu.configure_miniapp_url_provider(lambda: "https://example.test/app")
    desired = MenuButtonWebApp(
        text="🚀 Открыть приложение",
        web_app=WebAppInfo(url="https://example.test/app"),
    )
    queue = JobQueue()
    context = SimpleNamespace(
        bot=Bot(current_menu_button=desired),
        job_queue=queue,
    )

    assert run(retry.sync_public_commands_once(context, ("menu",), retry_callback)) is True
    assert context.bot.menu_read_calls == 1
    assert context.bot.menu_write_calls == []
    assert queue.calls == []


def test_public_profile_is_reconciled_before_menu_button():
    main_menu.configure_miniapp_url_provider(lambda: "https://example.test/app")
    queue = JobQueue()
    context = SimpleNamespace(
        bot=Bot(profile_values={}),
        job_queue=queue,
    )

    assert run(retry.sync_public_commands_once(context, ("menu",), retry_callback)) is True
    assert len(context.bot.profile_write_calls) == 6
    assert context.bot.menu_read_calls == 1
    assert queue.calls == []


def test_missing_main_mini_app_is_warning_only(caplog):
    queue = JobQueue()
    context = SimpleNamespace(
        bot=Bot(has_main_web_app=False, username="milovanovaibot"),
        job_queue=queue,
    )

    with caplog.at_level("WARNING"):
        assert run(retry.sync_public_commands_once(context, ("menu",), retry_callback)) is True

    assert "Main Mini App is not configured for @milovanovaibot" in caplog.text
    assert context.bot.command_calls == [("menu",)]
    assert context.bot.menu_read_calls == 0
    assert queue.calls == []


def test_configured_main_mini_app_is_logged_as_verified(caplog):
    queue = JobQueue()
    context = SimpleNamespace(
        bot=Bot(has_main_web_app=True, username="milovanovaibot"),
        job_queue=queue,
    )

    with caplog.at_level("INFO"):
        assert run(retry.sync_public_commands_once(context, ("menu",), retry_callback)) is True

    assert "Main Mini App verified for @milovanovaibot" in caplog.text
    assert context.bot.command_calls == [("menu",)]
    assert queue.calls == []


def test_retry_after_seconds_is_rescheduled_once():
    queue = JobQueue()
    context = SimpleNamespace(
        bot=Bot(command_error=RetryAfter(17)),
        job_queue=queue,
    )

    assert run(retry.sync_public_commands_once(context, ("menu",), retry_callback)) is False
    assert queue.calls == [(retry_callback, 17.0)]


def test_public_profile_retry_after_is_rescheduled_by_same_startup_job():
    queue = JobQueue()
    context = SimpleNamespace(
        bot=Bot(profile_read_error=RetryAfter(18)),
        job_queue=queue,
    )

    assert run(retry.sync_public_commands_once(context, ("menu",), retry_callback)) is False
    assert queue.calls == [(retry_callback, 18.0)]
    assert context.bot.menu_read_calls == 0


def test_menu_button_retry_after_is_rescheduled_by_same_startup_job():
    main_menu.configure_miniapp_url_provider(lambda: "https://example.test/app")
    queue = JobQueue()
    context = SimpleNamespace(
        bot=Bot(menu_read_error=RetryAfter(19)),
        job_queue=queue,
    )

    assert run(retry.sync_public_commands_once(context, ("menu",), retry_callback)) is False
    assert queue.calls == [(retry_callback, 19.0)]


def test_retry_after_timedelta_uses_shared_normalizer():
    queue = JobQueue()
    context = SimpleNamespace(
        bot=Bot(command_error=RetryAfter(timedelta(seconds=23))),
        job_queue=queue,
    )

    assert run(retry.sync_public_commands_once(context, ("menu",), retry_callback)) is False
    assert queue.calls == [(retry_callback, 23.0)]


def test_retry_after_without_job_queue_fails_closed():
    context = SimpleNamespace(
        bot=Bot(command_error=RetryAfter(5)),
        job_queue=None,
    )

    with pytest.raises(retry.CommandMenuRetryUnavailable, match="JobQueue"):
        run(retry.sync_public_commands_once(context, ("menu",), retry_callback))


def test_non_rate_limit_error_is_not_hidden_or_rescheduled():
    queue = JobQueue()
    context = SimpleNamespace(
        bot=Bot(command_error=RuntimeError("network")),
        job_queue=queue,
    )

    with pytest.raises(RuntimeError, match="network"):
        run(retry.sync_public_commands_once(context, ("menu",), retry_callback))
    assert queue.calls == []
