from pathlib import Path
from types import SimpleNamespace

import pytest

import telegram_answer_animation as answer_animation
import telegram_main_menu as menu
import telegram_quiz_result_menu as result_menu


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")

_EXPECTED_CALLBACKS = [
    "about",
    "start_test",
    "challenge_menu",
    "historical_menu",
    "battle_menu",
    "leaderboard",
    "my_stats",
    "my_status",
    "report_menu",
    "user_settings",
]


def setup_function():
    menu.configure_miniapp_url_provider(None)


def test_main_keyboard_preserves_historical_base_callbacks():
    keyboard = menu.main_keyboard()
    assert [row[0].callback_data for row in keyboard.inline_keyboard] == _EXPECTED_CALLBACKS
    assert all(row[0].web_app is None for row in keyboard.inline_keyboard)


def test_main_keyboard_prepends_miniapp_without_changing_base_callbacks():
    menu.configure_miniapp_url_provider(lambda: "https://example.test/app")

    keyboard = menu.main_keyboard()

    first = keyboard.inline_keyboard[0][0]
    assert first.text == "🚀 Открыть приложение"
    assert first.web_app.url == "https://example.test/app"
    assert [row[0].callback_data for row in keyboard.inline_keyboard[1:]] == _EXPECTED_CALLBACKS


def test_blank_miniapp_url_is_treated_as_unconfigured():
    menu.configure_miniapp_url_provider(lambda: "   ")
    keyboard = menu.main_keyboard()
    assert [row[0].callback_data for row in keyboard.inline_keyboard] == _EXPECTED_CALLBACKS


def test_legacy_bridge_points_transitional_callers_at_canonical_factories():
    def old_factory():
        return object()

    async def old_result_menu(bot, chat_id, data):
        return None

    async def old_animation(query, btn_index, correct_index, is_numeric_mode, shuffled):
        return None

    legacy = SimpleNamespace(
        _main_keyboard=old_factory,
        send_final_results_menu=old_result_menu,
        _animate_answer_buttons=old_animation,
    )

    menu.install_legacy_bridge(
        legacy,
        miniapp_url_provider=lambda: "https://example.test/app",
    )

    assert legacy._main_keyboard is menu.main_keyboard
    assert legacy.send_final_results_menu is result_menu.send_final_results_menu
    assert legacy._animate_answer_buttons is answer_animation.animate_answer_buttons
    keyboard = legacy._main_keyboard()
    assert keyboard.inline_keyboard[0][0].web_app.url == "https://example.test/app"


def test_legacy_bridge_fails_closed_without_expected_main_menu_seam():
    with pytest.raises(TypeError):
        menu.install_legacy_bridge(SimpleNamespace(), miniapp_url_provider=None)


def test_legacy_bridge_validates_result_menu_before_mutating_main_menu():
    def old_factory():
        return object()

    async def old_animation(query, btn_index, correct_index, is_numeric_mode, shuffled):
        return None

    legacy = SimpleNamespace(
        _main_keyboard=old_factory,
        send_final_results_menu=None,
        _animate_answer_buttons=old_animation,
    )

    with pytest.raises(TypeError):
        menu.install_legacy_bridge(
            legacy,
            miniapp_url_provider=lambda: "https://should-not-install.test/app",
        )

    assert legacy._main_keyboard is old_factory
    assert legacy.send_final_results_menu is None
    assert legacy._animate_answer_buttons is old_animation
    assert menu.main_keyboard().inline_keyboard[0][0].callback_data == "about"


def test_legacy_bridge_validates_animation_before_mutating_other_presenters():
    def old_factory():
        return object()

    async def old_result_menu(bot, chat_id, data):
        return None

    legacy = SimpleNamespace(
        _main_keyboard=old_factory,
        send_final_results_menu=old_result_menu,
        _animate_answer_buttons=None,
    )

    with pytest.raises(TypeError):
        menu.install_legacy_bridge(
            legacy,
            miniapp_url_provider=lambda: "https://should-not-install.test/app",
        )

    assert legacy._main_keyboard is old_factory
    assert legacy.send_final_results_menu is old_result_menu
    assert legacy._animate_answer_buttons is None
    assert menu.main_keyboard().inline_keyboard[0][0].callback_data == "about"


def test_invalid_provider_is_rejected():
    with pytest.raises(TypeError):
        menu.configure_miniapp_url_provider("https://example.test/app")


def test_production_uses_focused_main_menu_without_legacy_bridge():
    assert "import telegram_main_menu as main_menu" in PRODUCTION_SOURCE
    assert "main_menu.configure_miniapp_url_provider(_miniapp_url)" in PRODUCTION_SOURCE
    assert "main_menu.install_legacy_bridge" not in PRODUCTION_SOURCE
    assert "main_menu.main_keyboard" in PRODUCTION_SOURCE
    assert "import bot" not in PRODUCTION_SOURCE
