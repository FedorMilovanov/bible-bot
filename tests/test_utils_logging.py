from pathlib import Path

import utils


SOURCE = Path(utils.__file__).read_text(encoding="utf-8")


def test_utility_logging_redacts_identity_payload_and_tracebacks():
    assert "exc_info=True" not in SOURCE
    assert 'logger.debug("Avatar load failed for %d: %s"' not in SOURCE
    assert 'logger.error("generate_result_image error: %s"' not in SOURCE
    assert 'logger.error("create_result_gif error: %s"' not in SOURCE

    for forbidden in (
        'logger.error("safe_send plain fallback failed: %s"',
        'logger.error("safe_send BadRequest: %s"',
        'logger.error("safe_send failed: %s"',
        'logger.error("safe_edit plain fallback failed: %s"',
        'logger.error("safe_edit BadRequest: %s"',
        'logger.error("safe_edit failed: %s"',
        'logger.warning("safe_delete: %s"',
    ):
        assert forbidden not in SOURCE

    logger_lines = [line for line in SOURCE.splitlines() if "logger." in line]
    assert all("user_id" not in line for line in logger_lines)
    assert all("provider-sensitive-marker" not in line for line in logger_lines)


def test_utility_failure_logs_keep_operation_and_exception_class():
    assert 'logger.debug("Avatar load failed (%s)", type(exc).__name__)' in SOURCE
    assert 'logger.error("generate_result_image failed (%s)", type(exc).__name__)' in SOURCE
    assert 'logger.error("create_result_gif failed (%s)", type(exc).__name__)' in SOURCE
