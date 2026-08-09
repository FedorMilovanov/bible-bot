"""Production HTTP lifecycle for Mini App + API.

Imported by bot.py before bot configuration, so .env is loaded here first.
"""
from __future__ import annotations

import logging
import os
from threading import Lock, Thread

from dotenv import load_dotenv

load_dotenv()

from web_api import create_app  # noqa: E402  (env must be loaded first)

logger = logging.getLogger(__name__)
app = create_app()
_SERVER_LOCK = Lock()
_SERVER_STARTED = False


def run() -> None:
    """Run a production WSGI server in the current thread."""
    from waitress import serve

    port = int(os.getenv("PORT", "8080"))
    threads = max(2, int(os.getenv("WEB_THREADS", "4")))
    # This is the bounded server envelope for all HTTP requests, including
    # Telegram webhook Updates. Mini App quiz POSTs have a separate 64 KiB
    # per-request Flask limit in web_api.create_app().
    max_body = int(os.getenv("MAX_REQUEST_BODY_BYTES", str(1024 * 1024)))
    max_headers = int(os.getenv("MAX_REQUEST_HEADER_BYTES", str(64 * 1024)))
    logger.info(
        "HTTP server listening on 0.0.0.0:%s with %s threads (body<=%sB headers<=%sB)",
        port,
        threads,
        max_body,
        max_headers,
    )
    serve(
        app,
        host="0.0.0.0",
        port=port,
        threads=threads,
        channel_timeout=120,
        max_request_body_size=max_body,
        max_request_header_size=max_headers,
        clear_untrusted_proxy_headers=True,
        expose_tracebacks=False,
    )


def keep_alive() -> None:
    """Start the HTTP server exactly once in a daemon thread."""
    if os.getenv("DISABLE_WEB_SERVER", "false").lower() in {"1", "true", "yes"}:
        return

    global _SERVER_STARTED
    with _SERVER_LOCK:
        if _SERVER_STARTED:
            return
        Thread(target=run, daemon=True, name="MiniAppHTTP").start()
        _SERVER_STARTED = True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
