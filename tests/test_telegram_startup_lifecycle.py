import asyncio

from web_api import telegram_transport


class _RecordingBot:
    def __init__(self, events):
        self.events = events

    async def set_webhook(self, **kwargs):
        del kwargs
        self.events.append("webhook")
        return True


class _RecordingApplication:
    def __init__(self, events):
        self.events = events
        self.bot = _RecordingBot(events)
        self.update_queue = asyncio.Queue()

        async def post_init(application):
            assert application is self
            self.events.append("post_init")

        self.post_init = post_init

    async def __aenter__(self):
        self.events.append("initialize")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        del exc_type, exc, tb
        self.events.append("shutdown")

    async def start(self):
        self.events.append("start")

    async def stop(self):
        self.events.append("stop")


def test_custom_webhook_lifecycle_awaits_post_init_before_webhook_and_start(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "123456:TEST_TOKEN")
    monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", "https://example.com")
    monkeypatch.setenv("TELEGRAM_WEBHOOK_MAX_CONNECTIONS", "1")
    monkeypatch.delenv("TELEGRAM_WEBHOOK_SECRET", raising=False)
    events = []
    application = _RecordingApplication(events)

    async def scenario():
        stop_event = asyncio.Event()
        stop_event.set()
        await telegram_transport._run_webhook_application(
            application,
            stop_event=stop_event,
            install_signal_handlers=False,
        )

    asyncio.run(scenario())

    assert events == [
        "initialize",
        "post_init",
        "webhook",
        "start",
        "stop",
        "shutdown",
    ]
    assert telegram_transport.TELEGRAM_WEBHOOK_BRIDGE.ready() is False
