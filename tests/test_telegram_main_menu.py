from pathlib import Path

import pytest

import telegram_main_menu as menu


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
MENU_SOURCE = (ROOT / "telegram_main_menu.py").read_text(encoding="utf-8")

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


def test_main_keyboard_preserves_base_callbacks():
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


def test_invalid_provider_is_rejected():
    with pytest.raises(TypeError):
        menu.configure_miniapp_url_provider("https://example.test/app")


def test_main_menu_has_no_retired_monolith_bridge():
    assert "install_legacy_bridge" not in MENU_SOURCE
    assert "legacy_module" not in MENU_SOURCE
    assert "bot.py" not in MENU_SOURCE


def test_production_uses_focused_main_menu_authority_directly():
    assert "import telegram_main_menu as main_menu" in PRODUCTION_SOURCE
    assert "main_menu.configure_miniapp_url_provider(_miniapp_url)" in PRODUCTION_SOURCE
    assert "main_menu.main_keyboard" in PRODUCTION_SOURCE
    assert "main_menu.install_legacy_bridge" not in PRODUCTION_SOURCE
    assert "legacy._main_keyboard" not in PRODUCTION_SOURCE
    assert "legacy =" not in PRODUCTION_SOURCE
    assert PRODUCTION_SOURCE.index("def _miniapp_url()") < PRODUCTION_SOURCE.index(
        "main_menu.configure_miniapp_url_provider(_miniapp_url)"
    )
