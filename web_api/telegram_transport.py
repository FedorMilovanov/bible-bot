"""Telegram update transport for polling and Render-friendly custom webhook mode.

The existing Waitress/Flask service owns the public HTTP port. In webhook mode
it validates Telegram's secret header and forwards parsed updates into PTB's
``Application.update_queue`` instead of starting PTB's optional webhook server.
"""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import os
import re
import signal
from collections.abc import Awaitable, Callable
from concurrent.futures import TimeoutError as FutureTimeoutError
from threading import Lock
from urllib.parse import urlsplit

from telegram import Update

logger = logging.getLogger(__name__)

WEBHOOK_PATH = "/telegram/webhook"
_WEBHOOK_SECRET_RE = re.compile(r"^[A-Za-z0-9_-]{16,256}$")
_BRIDGE_ENQUEUE_TIMEOUT_SECONDS = 2.0


class TransportConfigurationError(RuntimeError):
    """Raised when Telegram transport environment configuration is unsafe."""


class WebhookNotReady(RuntimeError):
    """Raised while the HTTP server is up but PTB is not ready to accept updates."""


class InvalidWebhookUpdate(ValueError):
    """Raised when a webhook body cannot be decoded as a Telegram Update."""


def telegram_transport_mode() -> str:
    mode = os.getenv("TELEGRAM_TRANSPORT", "polling").strip().lower() or "polling"
    if mode not in {"polling", "webhook"}:
        raise TransportConfigurationError("TELEGRAM_TRANSPORT must be 'polling' or 'webhook'")
    return mode


def telegram_webhook_secret() -> str:
    explicit = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
    if explicit:
        if not _WEBHOOK_SECRET_RE.fullmatch(explicit):
            raise TransportConfigurationError(
                "TELEGRAM_WEBHOOK_SECRET must be 16-256 characters using only A-Z, a-z, 0-9, _ or -"
            )
        return explicit

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise TransportConfigurationError("BOT_TOKEN is required to derive the webhook secret")
    return hashlib.sha256(f"bible-bot-webhook:{bot_token}".encode()).hexdigest()


def telegram_webhook_base_url() -> str:
    raw = (
        os.getenv("TELEGRAM_WEBHOOK_BASE_URL", "").strip()
        or os.getenv("RENDER_EXTERNAL_URL", "").strip()
    )
    if not raw:
        raise TransportConfigurationError(
            "TELEGRAM_WEBHOOK_BASE_URL or RENDER_EXTERNAL_URL is required in webhook mode"
        )

    parsed = urlsplit(raw)
    if parsed.scheme != "https" or not parsed.netloc or parsed.query or parsed.fragment:
        raise TransportConfigurationError("Telegram webhook base URL must be an HTTPS origin/path without query or fragment")
    return raw.rstrip("/")


def telegram_webhook_url() -> str:
    return f"{telegram_webhook_base_url()}{WEBHOOK_PATH}"


def telegram_webhook_max_connections() -> int:
    raw = os.getenv("TELEGRAM_WEBHOOK_MAX_CONNECTIONS", "4").strip() or "4"
    try:
        value = int(raw)
    except ValueError as exc:
        raise TransportConfigurationError("TELEGRAM_WEBHOOK_MAX_CONNECTIONS must be an integer from 1 to 100") from exc
    if value < 1 or value > 100:
        raise TransportConfigurationError("TELEGRAM_WEBHOOK_MAX_CONNECTIONS must be an integer from 1 to 100")
    return value


class TelegramWebhookBridge:
    """Thread-safe bridge from Waitress threads to PTB's asyncio update queue."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._application = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def configure(self, application, loop: asyncio.AbstractEventLoop) -> None:
        if loop.is_closed():
            raise RuntimeError("cannot configure Telegram webhook bridge with a closed event loop")
        with self._lock:
            self._application = application
            self._loop = loop

    def clear(self, application=None) -> None:
        with self._lock:
            if application is not None and self._application is not application:
                return
            self._application = None
            self._loop = None

    def ready(self) -> bool:
        with self._lock:
            return self._application is not None and self._loop is not None and not self._loop.is_closed()

    def submit(self, payload: dict) -> None:
        with self._lock:
            application = self._application
            loop = self._loop

        if application is None or loop is None or loop.is_closed():
            raise WebhookNotReady("Telegram application is not ready")

        try:
            update = Update.de_json(data=payload, bot=application.bot)
        except Exception as exc:
            raise InvalidWebhookUpdate("invalid Telegram update") from exc
        if update is None:
            raise InvalidWebhookUpdate("invalid Telegram update")

        future = asyncio.run_coroutine_threadsafe(application.update_queue.put(update), loop)
        try:
            future.result(timeout=_BRIDGE_ENQUEUE_TIMEOUT_SECONDS)
        except FutureTimeoutError as exc:
            future.cancel()
            raise WebhookNotReady("Telegram update queue did not accept the update in time") from exc
        except Exception as exc:
            raise WebhookNotReady("Telegram update queue is unavailable") from exc


TELEGRAM_WEBHOOK_BRIDGE = TelegramWebhookBridge()


async def _call_shutdown_hook(callback: Callable[[], Awaitable[None] | None] | None) -> None:
    if callback is None:
        return
    result = callback()
    if inspect.isawaitable(result):
        await result


async def _run_webhook_application(
    application,
    *,
    before_shutdown: Callable[[], Awaitable[None] | None] | None = None,
    stop_event: asyncio.Event | None = None,
    install_signal_handlers: bool = True,
) -> None:
    """Run PTB lifecycle while Waitress owns the public HTTP socket."""
    webhook_url = telegram_webhook_url()
    secret = telegram_webhook_secret()
    max_connections = telegram_webhook_max_connections()
    local_stop = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []

    if install_signal_handlers:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, local_stop.set)
                installed_signals.append(sig)
            except (NotImplementedError, RuntimeError):
                logger.debug("async signal handler unavailable for %s", sig)

    started = False
    try:
        async with application:
            TELEGRAM_WEBHOOK_BRIDGE.configure(application, loop)
            configured = await application.bot.set_webhook(
                url=webhook_url,
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=False,
                max_connections=max_connections,
                secret_token=secret,
            )
            if configured is not True:
                raise RuntimeError("Telegram did not confirm webhook registration")
            await application.start()
            started = True
            logger.info(
                "Telegram webhook transport active at %s (max_connections=%s)",
                WEBHOOK_PATH,
                max_connections,
            )
            await local_stop.wait()
            await _call_shutdown_hook(before_shutdown)
            if started:
                await application.stop()
                started = False
    finally:
        TELEGRAM_WEBHOOK_BRIDGE.clear(application)
        if started:
            try:
                await application.stop()
            except Exception:
                logger.exception("failed to stop Telegram application cleanly")
        for sig in installed_signals:
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError):
                pass


def run_telegram_application(application, *, before_shutdown=None) -> None:
    """Run the configured Telegram transport without changing handler setup."""
    mode = telegram_transport_mode()
    if mode == "polling":
        application.run_polling()
        return
    asyncio.run(_run_webhook_application(application, before_shutdown=before_shutdown))
