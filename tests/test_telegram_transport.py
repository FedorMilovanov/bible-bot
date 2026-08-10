import asyncio
from threading import Event as ThreadEvent
from threading import Thread

import pytest

from web_api import telegram_transport


class FakeBot:
    def __init__(self, *, webhook_result=True):
        self.webhook_calls = []
        self.webhook_result = webhook_result

    async def set_webhook(self, **kwargs):
        self.webhook_calls.append(kwargs)
        return self.webhook_result


class FakeApplication:
    def __init__(self, *, webhook_result=True):
        self.bot = FakeBot(webhook_result=webhook_result)
        self.update_queue = asyncio.Queue()
        self.events = []

    async def __aenter__(self):
        self.events.append("initialize")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.events.append("shutdown")

    async def start(self):
        self.events.append("start")

    async def stop(self):
        self.events.append("stop")


class RecordingQueue:
    def __init__(self):
        self.items = []

    async def put(self, item):
        self.items.append(item)


class BlockingQueue:
    def __init__(self):
        self.items = []
        self.entered = ThreadEvent()
        self.release = ThreadEvent()

    async def put(self, item):
        self.entered.set()
        await asyncio.to_thread(self.release.wait)
        self.items.append(item)


def test_transport_defaults_to_polling(monkeypatch):
    monkeypatch.delenv("TELEGRAM_TRANSPORT", raising=False)
    assert telegram_transport.telegram_transport_mode() == "polling"


def test_polling_mode_delegates_to_legacy_run_polling(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "polling")

    class PollingApplication:
        def __init__(self):
            self.called = False

        def run_polling(self):
            self.called = True

    app = PollingApplication()
    telegram_transport.run_telegram_application(app)
    assert app.called is True


def test_invalid_transport_mode_fails_closed(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TRANSPORT", "magic")
    with pytest.raises(telegram_transport.TransportConfigurationError):
        telegram_transport.telegram_transport_mode()


def test_webhook_secret_is_stable_allowed_hex_when_derived(monkeypatch):
    monkeypatch.delenv("TELEGRAM_WEBHOOK_SECRET", raising=False)
    monkeypatch.setenv("BOT_TOKEN", "123456:TEST_TOKEN")

    first = telegram_transport.telegram_webhook_secret()
    second = telegram_transport.telegram_webhook_secret()

    assert first == second
    assert len(first) == 64
    assert first.isalnum()


def test_explicit_webhook_secret_rejects_telegram_invalid_characters(monkeypatch):
    monkeypatch.setenv("TELEGRAM_WEBHOOK_SECRET", "invalid=base64=secret")
    with pytest.raises(telegram_transport.TransportConfigurationError):
        telegram_transport.telegram_webhook_secret()


def test_webhook_url_uses_render_external_url(monkeypatch):
    monkeypatch.delenv("TELEGRAM_WEBHOOK_BASE_URL", raising=False)
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bible-bot.onrender.com/")

    assert telegram_transport.telegram_webhook_url() == "https://bible-bot.onrender.com/telegram/webhook"


def test_webhook_base_url_requires_https_and_no_query(monkeypatch):
    monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", "http://example.com")
    with pytest.raises(telegram_transport.TransportConfigurationError):
        telegram_transport.telegram_webhook_base_url()

    monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", "https://example.com/?x=1")
    with pytest.raises(telegram_transport.TransportConfigurationError):
        telegram_transport.telegram_webhook_base_url()


def test_webhook_connections_default_to_four_and_validate_bot_api_range(monkeypatch):
    monkeypatch.delenv("TELEGRAM_WEBHOOK_MAX_CONNECTIONS", raising=False)
    assert telegram_transport.telegram_webhook_max_connections() == 4

    for invalid in ("0", "101", "many"):
        monkeypatch.setenv("TELEGRAM_WEBHOOK_MAX_CONNECTIONS", invalid)
        with pytest.raises(telegram_transport.TransportConfigurationError):
            telegram_transport.telegram_webhook_max_connections()

    monkeypatch.setenv("TELEGRAM_WEBHOOK_MAX_CONNECTIONS", "8")
    assert telegram_transport.telegram_webhook_max_connections() == 8


def test_bridge_parses_real_minimal_update_and_puts_it_on_ptb_queue():
    loop = asyncio.new_event_loop()
    thread = Thread(target=loop.run_forever, daemon=True)
    thread.start()
    queue = RecordingQueue()
    app = type("BridgeApplication", (), {"bot": FakeBot(), "update_queue": queue})()

    try:
        telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.configure(app, loop)
        telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.submit({"update_id": 42})
    finally:
        telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.clear(app)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=2)
        loop.close()

    assert len(queue.items) == 1
    assert queue.items[0].update_id == 42


@pytest.mark.parametrize("payload", [{}, {"update_id": -1}, {"update_id": True}])
def test_bridge_rejects_update_without_valid_update_id(payload):
    loop = asyncio.new_event_loop()
    thread = Thread(target=loop.run_forever, daemon=True)
    thread.start()
    queue = RecordingQueue()
    app = type("BridgeApplication", (), {"bot": FakeBot(), "update_queue": queue})()

    try:
        telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.configure(app, loop)
        with pytest.raises(telegram_transport.InvalidWebhookUpdate):
            telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.submit(payload)
    finally:
        telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.clear(app)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=2)
        loop.close()

    assert queue.items == []


def test_bridge_deactivation_waits_for_inflight_submission_and_rejects_new_ones():
    loop = asyncio.new_event_loop()
    loop_thread = Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()
    queue = BlockingQueue()
    app = type("BridgeApplication", (), {"bot": FakeBot(), "update_queue": queue})()
    errors = []

    telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.configure(app, loop)

    def submit_inflight():
        try:
            telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.submit({"update_id": 100})
        except Exception as exc:  # pragma: no cover - assertion reports the exception below
            errors.append(exc)

    submit_thread = Thread(target=submit_inflight, daemon=True)
    submit_thread.start()
    assert queue.entered.wait(timeout=1)

    async def scenario():
        drain = asyncio.create_task(
            telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.deactivate_and_drain(app, timeout=1)
        )
        await asyncio.sleep(0.05)
        assert drain.done() is False
        with pytest.raises(telegram_transport.WebhookNotReady):
            telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.submit({"update_id": 101})
        queue.release.set()
        assert await drain is True

    try:
        asyncio.run(scenario())
        submit_thread.join(timeout=2)
    finally:
        queue.release.set()
        telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.clear(app)
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=2)
        loop.close()

    assert errors == []
    assert [item.update_id for item in queue.items] == [100]


def test_webhook_application_lifecycle_sets_webhook_and_preserves_it_on_shutdown(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "123456:TEST_TOKEN")
    monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", "https://example.com")
    monkeypatch.setenv("TELEGRAM_WEBHOOK_MAX_CONNECTIONS", "4")
    monkeypatch.delenv("TELEGRAM_WEBHOOK_SECRET", raising=False)

    class StartGuardApplication(FakeApplication):
        async def start(self):
            assert telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.ready() is False
            await super().start()

    app = StartGuardApplication()

    async def scenario():
        stop_event = asyncio.Event()
        stop_event.set()

        async def before_shutdown():
            assert telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.ready() is False
            app.events.append("saved")

        await telegram_transport._run_webhook_application(
            app,
            before_shutdown=before_shutdown,
            stop_event=stop_event,
            install_signal_handlers=False,
        )

    asyncio.run(scenario())

    assert app.events == ["initialize", "start", "stop", "saved", "shutdown"]
    assert len(app.bot.webhook_calls) == 1
    webhook = app.bot.webhook_calls[0]
    assert webhook["url"] == "https://example.com/telegram/webhook"
    assert webhook["drop_pending_updates"] is False
    assert webhook["max_connections"] == 4
    assert webhook["secret_token"] == telegram_transport.telegram_webhook_secret()
    assert telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.ready() is False


def test_webhook_application_fails_before_start_when_telegram_rejects_registration(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "123456:TEST_TOKEN")
    monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", "https://example.com")
    app = FakeApplication(webhook_result=False)

    async def scenario():
        stop_event = asyncio.Event()
        stop_event.set()
        await telegram_transport._run_webhook_application(
            app,
            stop_event=stop_event,
            install_signal_handlers=False,
        )

    with pytest.raises(RuntimeError, match="did not confirm webhook registration"):
        asyncio.run(scenario())

    assert app.events == ["initialize", "shutdown"]
    assert telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.ready() is False
