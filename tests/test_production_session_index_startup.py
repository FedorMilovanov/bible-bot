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
from web_api.db_hardening import MiniAppIndexSafetyUnavailable


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_storage_index_guards_run_before_application_builder():
    assert "from legacy_session_access import ensure_active_session_unique_index" in SOURCE
    assert "ensure_miniapp_indexes" in SOURCE
    active_index = SOURCE.index("ensure_active_session_unique_index()")
    miniapp_index = SOURCE.index("ensure_miniapp_indexes()")
    builder = SOURCE.index("Application.builder()")
    assert active_index < miniapp_index < builder


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
def test_production_startup_fails_before_miniapp_and_polling_when_quiz_index_is_unsafe(
    monkeypatch,
    error,
):
    monkeypatch.setenv("BOT_TOKEN", "123456:TEST_TOKEN")
    monkeypatch.setenv("MONGO_URL", "mongodb://configured")

    def unsafe_index():
        raise error

    monkeypatch.setattr(production, "ensure_active_session_unique_index", unsafe_index)
    monkeypatch.setattr(
        production,
        "ensure_miniapp_indexes",
        lambda: (_ for _ in ()).throw(AssertionError("Mini App hardening must not run")),
    )
    monkeypatch.setattr(
        production.Application,
        "builder",
        lambda: (_ for _ in ()).throw(AssertionError("Application must not be built")),
    )

    with pytest.raises(type(error), match=str(error)):
        production.main()


def test_production_startup_fails_before_polling_when_miniapp_indexes_are_unsafe(
    monkeypatch,
):
    monkeypatch.setenv("BOT_TOKEN", "123456:TEST_TOKEN")
    monkeypatch.setenv("MONGO_URL", "mongodb://configured")
    calls = []

    monkeypatch.setattr(
        production,
        "ensure_active_session_unique_index",
        lambda: calls.append("quiz") or True,
    )

    def unsafe_miniapp():
        calls.append("miniapp")
        raise MiniAppIndexSafetyUnavailable("duplicate open Mini App sessions")

    monkeypatch.setattr(production, "ensure_miniapp_indexes", unsafe_miniapp)
    monkeypatch.setattr(
        production.Application,
        "builder",
        lambda: (_ for _ in ()).throw(AssertionError("Application must not be built")),
    )

    with pytest.raises(MiniAppIndexSafetyUnavailable, match="duplicate open Mini App"):
        production.main()
    assert calls == ["quiz", "miniapp"]


def test_false_miniapp_hardening_result_is_not_accepted_as_safe(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "123456:TEST_TOKEN")
    monkeypatch.setenv("MONGO_URL", "mongodb://configured")
    monkeypatch.setattr(production, "ensure_active_session_unique_index", lambda: True)
    monkeypatch.setattr(production, "ensure_miniapp_indexes", lambda: False)
    monkeypatch.setattr(
        production.Application,
        "builder",
        lambda: (_ for _ in ()).throw(AssertionError("Application must not be built")),
    )

    with pytest.raises(MiniAppIndexSafetyUnavailable, match="safety is unavailable"):
        production.main()
