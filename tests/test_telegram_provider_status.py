from types import SimpleNamespace

from telegram_provider_status import main_mini_app_status


def test_main_mini_app_status_normalizes_enabled_bot():
    assert main_mini_app_status(
        SimpleNamespace(username="@milovanovaibot", has_main_web_app=True)
    ) == {
        "username": "milovanovaibot",
        "has_main_web_app": True,
    }


def test_main_mini_app_status_treats_absent_optional_flag_as_disabled():
    assert main_mini_app_status(SimpleNamespace(username="milovanovaibot")) == {
        "username": "milovanovaibot",
        "has_main_web_app": False,
    }


def test_main_mini_app_status_treats_none_flag_as_disabled():
    assert main_mini_app_status(
        SimpleNamespace(username="milovanovaibot", has_main_web_app=None)
    ) == {
        "username": "milovanovaibot",
        "has_main_web_app": False,
    }
