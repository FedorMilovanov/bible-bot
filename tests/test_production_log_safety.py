from __future__ import annotations

import os
import subprocess
import sys


def _run_python(code: str, *, app_env: str | None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if app_env is None:
        env.pop("APP_ENV", None)
    else:
        env["APP_ENV"] = app_env
    return subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def test_production_startup_suppresses_http_client_info_urls():
    result = _run_python(
        "import logging; "
        "logging.basicConfig(level=logging.INFO); "
        "logging.getLogger('httpx').info('SENSITIVE_REQUEST_URL'); "
        "logging.getLogger('app').info('APPLICATION_LOG_OK')",
        app_env="production",
    )
    assert result.returncode == 0
    assert "SENSITIVE_REQUEST_URL" not in result.stderr
    assert "APPLICATION_LOG_OK" in result.stderr


def test_nonproduction_startup_does_not_force_http_client_level():
    result = _run_python(
        "import logging; "
        "logging.basicConfig(level=logging.INFO); "
        "logging.getLogger('httpx').info('HTTPX_INFO_VISIBLE')",
        app_env=None,
    )
    assert result.returncode == 0
    assert "HTTPX_INFO_VISIBLE" in result.stderr
