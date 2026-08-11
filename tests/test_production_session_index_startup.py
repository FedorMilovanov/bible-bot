import os
from pathlib import Path

import pytest

os.environ.setdefault("ADMIN_USER_ID", "1")
os.environ.setdefault("DISABLE_WEB_SERVER", "true")

import telegram_production as production
from legacy_session_access import (
    QuizSessionAccessSchemaInvalid,
    QuizSessionAccessUnavailable,
)


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_unique_active_session_guard_runs_before_application_builder():
    assert "from legacy_session_access import ensure_active_session_unique_index" in SOURCE
    assert SOURCE.index("ensure_active_session_unique_index()") < SOURCE.index(
        "Application.builder()"
    )


@pytest.mark.parametrize(
    "error",
    [
        QuizSessionAccessSchemaInvalid(
            "duplicate active sessions prevent unique-index creation"
        ),
        QuizSessionAccessUnavailable(
            "active-session unique-index preparation failed"
        ),
    ],
)
def test_production_startup_fails_before_polling_when_unique_index_is_unsafe(
    monkeypatch,
    error,
):
    monkeypatch.setenv("BOT_TOKEN", "123456:TEST_TOKEN")
    monkeypatch.setenv("MONGO_URL", "mongodb://configured")

    def unsafe_index():
        raise error

    monkeypatch.setattr(production, "ensure_active_session_unique_index", unsafe_index)
    monkeypatch.setattr(
        production.Application,
        "builder",
        lambda: (_ for _ in ()).throw(AssertionError("Application must not be built")),
    )

    with pytest.raises(type(error), match=str(error)):
        production.main()
