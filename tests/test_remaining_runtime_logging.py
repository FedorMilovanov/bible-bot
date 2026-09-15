from pathlib import Path

import web_api.routes as routes


ROOT = Path(__file__).resolve().parents[1]
MODULES = (
    "telegram_challenge_controller.py",
    "telegram_course_surface.py",
    "telegram_production.py",
    "web_api/__init__.py",
    "web_api/routes.py",
    "web_api/telegram_transport.py",
)


def test_remaining_runtime_modules_have_no_traceback_logging():
    for relative in MODULES:
        source = (ROOT / relative).read_text(encoding="utf-8")
        assert "logger.exception(" not in source
        assert "exc_info=True" not in source


def test_runtime_logging_helper_redacts_exception_payload(caplog):
    marker = "provider-sensitive-marker"

    with caplog.at_level("ERROR", logger=routes.__name__):
        routes._log_runtime_failure(
            "runtime boundary failed",
            RuntimeError(marker),
        )

    assert "runtime boundary failed (RuntimeError)" in caplog.text
    assert marker not in caplog.text
