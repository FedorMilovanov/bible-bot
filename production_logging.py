"""Production logging policy for third-party HTTP clients."""
from __future__ import annotations

import logging


def configure_production_logging() -> None:
    """Keep HTTP client request URLs out of normal production INFO logs."""
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
