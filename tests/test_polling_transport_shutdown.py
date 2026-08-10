import pytest

from web_api import telegram_transport


def test_polling_runs_persistence_hook_after_ptb_returns(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "polling")
    events = []

    class PollingApplication:
        def run_polling(self):
            events.append("polling_stopped")

    async def save_sessions():
        events.append("saved")

    telegram_transport.run_telegram_application(
        PollingApplication(),
        before_shutdown=save_sessions,
    )

    assert events == ["polling_stopped", "saved"]


def test_polling_still_persists_when_run_polling_raises(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "polling")
    events = []

    class PollingApplication:
        def run_polling(self):
            events.append("polling_failed")
            raise RuntimeError("polling failed")

    async def save_sessions():
        events.append("saved")

    with pytest.raises(RuntimeError, match="polling failed"):
        telegram_transport.run_telegram_application(
            PollingApplication(),
            before_shutdown=save_sessions,
        )

    assert events == ["polling_failed", "saved"]
