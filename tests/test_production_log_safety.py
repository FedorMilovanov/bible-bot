from __future__ import annotations

import os
import subprocess
import sys


def _run_python(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )


def test_production_logging_suppresses_http_client_info_urls():
    result = _run_python(
        "import logging; "
        "from production_logging import configure_production_logging; "
        "configure_production_logging(); "
        "logging.basicConfig(level=logging.INFO); "
        "logging.getLogger('httpx').info('SENSITIVE_REQUEST_URL'); "
        "logging.getLogger('app').info('APPLICATION_LOG_OK')"
    )
    assert result.returncode == 0
    assert "SENSITIVE_REQUEST_URL" not in result.stderr
    assert "APPLICATION_LOG_OK" in result.stderr


def test_production_logging_keeps_http_client_warnings_visible():
    result = _run_python(
        "import logging; "
        "from production_logging import configure_production_logging; "
        "configure_production_logging(); "
        "logging.basicConfig(level=logging.INFO); "
        "logging.getLogger('httpx').warning('NETWORK_WARNING_VISIBLE')"
    )
    assert result.returncode == 0
    assert "NETWORK_WARNING_VISIBLE" in result.stderr
