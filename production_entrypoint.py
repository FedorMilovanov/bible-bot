"""Production process bootstrap before importing the Telegram composition root."""
from __future__ import annotations

from production_logging import configure_production_logging

configure_production_logging()

from telegram_production import main  # noqa: E402


if __name__ == "__main__":
    main()
