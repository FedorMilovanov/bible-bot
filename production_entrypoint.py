"""Production process bootstrap before importing the Telegram composition root."""
from __future__ import annotations

from production_logging import configure_production_logging

configure_production_logging()

from legacy_session_retention import (  # noqa: E402
    QuizSessionRetentionUnavailable,
    ensure_state_aware_session_ttl,
)
from telegram_production import main as telegram_main  # noqa: E402


def main() -> None:
    """Fail closed unless legacy quiz recovery evidence has safe retention."""
    try:
        migrated = ensure_state_aware_session_ttl()
    except QuizSessionRetentionUnavailable:
        raise
    if migrated is not True:
        raise QuizSessionRetentionUnavailable(
            "quiz-session retention safety is unavailable"
        )
    telegram_main()


if __name__ == "__main__":
    main()
