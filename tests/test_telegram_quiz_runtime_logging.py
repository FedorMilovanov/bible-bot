import logging
from pathlib import Path

import telegram_quiz_runtime_controller as runtime


SOURCE = Path(runtime.__file__).read_text(encoding="utf-8")


def test_quiz_runtime_logging_redacts_exception_payload(caplog):
    with caplog.at_level(logging.WARNING, logger=runtime.__name__):
        runtime._log_quiz_failure(
            "question Telegram delivery failed",
            RuntimeError("provider-sensitive-marker"),
        )

    assert "question Telegram delivery failed (RuntimeError)" in caplog.text
    assert "provider-sensitive-marker" not in caplog.text


def test_quiz_runtime_source_has_no_traceback_or_identity_logging():
    assert "logger.exception(" not in SOURCE
    assert "exc_info=True" not in SOURCE
    for line in SOURCE.splitlines():
        if "logger." in line or "_log_quiz_failure(" in line:
            assert "user_id" not in line
            assert "chat_id" not in line
            assert " uid" not in line
