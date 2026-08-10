from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_render_enables_webhook_transport_on_existing_web_service():
    render = read("render.yaml")

    assert "startCommand: python bot.py" in render
    assert "healthCheckPath: /live" in render
    assert "- key: TELEGRAM_TRANSPORT\n        value: webhook" in render
    assert "- key: TELEGRAM_WEBHOOK_MAX_CONNECTIONS\n        value: \"2\"" in render
    assert "- key: WEB_THREADS\n        value: \"4\"" in render
    assert "TELEGRAM_WEBHOOK_BASE_URL" not in render
    assert "TELEGRAM_WEBHOOK_SECRET" not in render


def test_render_reserves_http_threads_beside_telegram_delivery():
    render = read("render.yaml")

    assert "- key: TELEGRAM_WEBHOOK_MAX_CONNECTIONS\n        value: \"2\"" in render
    assert "- key: WEB_THREADS\n        value: \"4\"" in render


def test_render_separates_webhook_envelope_from_miniapp_body_limit():
    render = read("render.yaml")

    assert "- key: MAX_REQUEST_BODY_BYTES\n        value: \"1048576\"" in render
    assert "- key: MINIAPP_MAX_REQUEST_BODY_BYTES\n        value: \"65536\"" in render
    assert "- key: MAX_REQUEST_HEADER_BYTES\n        value: \"65536\"" in render


def test_bot_launcher_uses_configurable_transport_and_shutdown_hook():
    bot = read("bot.py")

    assert "from web_api.telegram_transport import run_telegram_application" in bot
    assert "run_telegram_application(app, before_shutdown=_save_all_sessions)" in bot
    assert "app.run_polling()" not in bot
    assert "def _handle_shutdown" not in bot
    assert "signal.signal(signal.SIG" not in bot


def test_custom_webhook_uses_existing_flask_waitress_stack_without_ptb_webhook_extra():
    requirements = read("requirements.txt")
    transport = read("web_api/telegram_transport.py")

    assert "python-telegram-bot[job-queue]==20.7" in requirements
    assert "python-telegram-bot[webhooks]" not in requirements
    assert "Application.update_queue" in transport
    assert "run_webhook(" not in transport
