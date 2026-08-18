import ast
import asyncio
from pathlib import Path

from web_api import telegram_transport


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_ROOT = ROOT / "telegram_production.py"


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


def _call_attribute_name(node):
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def test_production_provider_sync_is_post_init_not_zero_delay_job():
    tree = ast.parse(PRODUCTION_ROOT.read_text(encoding="utf-8"))

    post_init_callbacks = []
    zero_delay_provider_jobs = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        attribute = _call_attribute_name(node.func)
        if attribute == "post_init" and node.args:
            callback = node.args[0]
            if isinstance(callback, ast.Name):
                post_init_callbacks.append(callback.id)
        if attribute != "run_once" or not node.args:
            continue
        callback = node.args[0]
        if not isinstance(callback, ast.Name) or callback.id != "_sync_public_command_menu_job":
            continue
        when_values = [
            keyword.value
            for keyword in node.keywords
            if keyword.arg == "when"
        ]
        if any(isinstance(value, ast.Constant) and value.value == 0 for value in when_values):
            zero_delay_provider_jobs.append(node.lineno)

    assert post_init_callbacks == ["_sync_public_command_menu_post_init"]
    assert zero_delay_provider_jobs == []
