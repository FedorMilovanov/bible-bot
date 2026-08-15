import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_production_surfaces_opt_into_ptb_timedelta_mode():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    render = (ROOT / "render.yaml").read_text(encoding="utf-8")
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "PTB_TIMEDELTA=1" in dockerfile
    assert "- key: PTB_TIMEDELTA\n        value: \"1\"" in render
    assert 'PTB_TIMEDELTA: "1"' in ci


def test_ptb_timedelta_opt_in_matches_retry_adapter_contract():
    env = os.environ.copy()
    env["PTB_TIMEDELTA"] = "1"
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from datetime import timedelta; "
                "from telegram.error import RetryAfter; "
                "assert isinstance(RetryAfter(3).retry_after, timedelta)"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert probe.returncode == 0, probe.stderr or probe.stdout
