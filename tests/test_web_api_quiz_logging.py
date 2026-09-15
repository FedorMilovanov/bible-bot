import logging
from pathlib import Path

from web_api import quiz
from web_api import quiz_start


def _source(module) -> str:
    return Path(module.__file__).read_text(encoding="utf-8")


def test_web_quiz_logging_redacts_exception_payloads(caplog):
    marker = "provider-sensitive-marker"

    with caplog.at_level(logging.ERROR, logger=quiz.__name__):
        quiz._log_quiz_failure("Mini App quiz failure", RuntimeError(marker))
    with caplog.at_level(logging.ERROR, logger=quiz_start.__name__):
        quiz_start._log_start_failure("Mini App quiz-start failure", RuntimeError(marker))

    assert "Mini App quiz failure (RuntimeError)" in caplog.text
    assert "Mini App quiz-start failure (RuntimeError)" in caplog.text
    assert marker not in caplog.text


def test_web_quiz_sources_have_no_traceback_or_identity_logging():
    for module in (quiz, quiz_start):
        source = _source(module)
        assert "logger.exception(" not in source
        assert "exc_info=True" not in source
        assert "open Mini App session already exists for user" not in source
        for line in source.splitlines():
            if "logger." in line or "_log_quiz_failure(" in line or "_log_start_failure(" in line:
                assert "result_id" not in line
                assert 'user["id"]' not in line
