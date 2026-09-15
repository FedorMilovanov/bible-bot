from pathlib import Path

import legacy_result_card_delivery as result_store
import legacy_session_close as session_close
import session_integrity as sessions
import telegram_result_delivery_controller as result_controller


def _source(module) -> str:
    return Path(module.__file__).read_text(encoding="utf-8")


def test_session_result_modules_do_not_emit_traceback_or_identity_logs():
    for module in (sessions, session_close, result_store, result_controller):
        source = _source(module)
        assert "logger.exception(" not in source
        assert "exc_info=True" not in source
        for line in source.splitlines():
            if "logger." in line or ("_log_" in line and "failure(" in line):
                assert "session_id" not in line
                assert "user_id" not in line


def test_logging_helpers_redact_exception_payload(caplog):
    marker = "provider-sensitive-marker"

    with caplog.at_level("ERROR", logger=result_store.__name__):
        result_store._log_result_card_failure(
            "result-card storage failed",
            RuntimeError(marker),
        )

    assert "result-card storage failed (RuntimeError)" in caplog.text
    assert marker not in caplog.text
