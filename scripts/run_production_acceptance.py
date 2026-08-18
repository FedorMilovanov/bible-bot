#!/usr/bin/env python3
"""Read-only production acceptance orchestrator.

This script never deploys, merges, changes Telegram webhook state or mutates MongoDB.
It only composes the repository's existing read-only preflights with exact public
HTTP deployment checks.

Exit codes:
  0 - every check for the selected phase is safe
  1 - an external system was reachable but at least one contract is unsafe
  2 - one or more contracts could not be established

Usage:
  python scripts/run_production_acceptance.py predeploy
  EXPECTED_DEPLOY_SHA=<40-hex-sha> python scripts/run_production_acceptance.py postdeploy
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

SAFE = 0
UNSAFE = 1
UNAVAILABLE = 2
ROOT = Path(__file__).resolve().parents[1]
_HTTP_TIMEOUT_SECONDS = 10.0
_SHA_RE = re.compile(r"[0-9a-f]{40}\Z")

PREDEPLOY_SCRIPTS = (
    "scripts/check_active_session_duplicates.py",
    "scripts/check_miniapp_session_duplicates.py",
    "scripts/check_session_unique_indexes.py",
    "scripts/check_retention_indexes.py",
    "scripts/check_result_storage_growth.py",
)
POSTDEPLOY_SCRIPTS = (
    "scripts/check_retention_indexes.py",
    "scripts/check_telegram_webhook.py",
    "scripts/check_telegram_main_app.py",
    "scripts/check_telegram_public_profile.py",
)


@dataclass(frozen=True)
class CheckResult:
    name: str
    code: int
    detail: str


def _normalize_code(code: int) -> int:
    if code == SAFE:
        return SAFE
    if code == UNSAFE:
        return UNSAFE
    return UNAVAILABLE


def _combine_codes(results: list[CheckResult]) -> int:
    codes = {_normalize_code(item.code) for item in results}
    if UNAVAILABLE in codes:
        return UNAVAILABLE
    if UNSAFE in codes:
        return UNSAFE
    return SAFE


def _compact_output(stdout: str, stderr: str) -> str:
    raw = (stdout.strip() or stderr.strip() or "no output").replace("\x00", "")
    # Bound operator output and keep secrets from accidentally being echoed by a
    # future child script. Existing preflights already avoid printing secrets.
    return raw[:2000]


def _run_script(path: str) -> CheckResult:
    completed = subprocess.run(
        [sys.executable, str(ROOT / path)],
        cwd=ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    return CheckResult(
        name=path,
        code=_normalize_code(completed.returncode),
        detail=_compact_output(completed.stdout, completed.stderr),
    )


def _deployment_origin() -> str:
    raw = (
        os.getenv("TELEGRAM_WEBHOOK_BASE_URL", "").strip()
        or os.getenv("RENDER_EXTERNAL_URL", "").strip()
    )
    if not raw:
        raise ValueError("TELEGRAM_WEBHOOK_BASE_URL or RENDER_EXTERNAL_URL is required")
    parsed = urlsplit(raw)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("production base URL must be an HTTPS origin")
    return raw.rstrip("/")


def _expected_deploy_sha() -> str:
    value = os.getenv("EXPECTED_DEPLOY_SHA", "").strip().lower()
    if _SHA_RE.fullmatch(value) is None:
        raise ValueError("EXPECTED_DEPLOY_SHA must be the exact 40-hex deployed revision")
    return value


def _fetch_json(origin: str, path: str) -> dict:
    request = Request(
        f"{origin}{path}",
        headers={"Accept": "application/json"},
        method="GET",
    )
    with urlopen(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
        status = int(response.status)
        payload = json.loads(response.read().decode("utf-8"))
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"{path} did not return HTTP 200 JSON object")
    return payload


def _http_contracts() -> list[CheckResult]:
    try:
        origin = _deployment_origin()
        expected_sha = _expected_deploy_sha()
    except ValueError as exc:
        return [CheckResult("deployment_http", UNAVAILABLE, str(exc))]

    results: list[CheckResult] = []
    contracts = (
        ("/live", lambda data: data.get("status") == "ok", "status=ok"),
        (
            "/ready",
            lambda data: data.get("status") == "ready" and data.get("database") is True,
            "status=ready database=true",
        ),
        (
            "/telegram/ready",
            lambda data: data.get("status") == "ready" and data.get("transport") == "webhook",
            "status=ready transport=webhook",
        ),
    )
    for path, predicate, expected in contracts:
        try:
            payload = _fetch_json(origin, path)
        except (HTTPError, URLError, TimeoutError, OSError, RuntimeError, json.JSONDecodeError) as exc:
            results.append(CheckResult(path, UNAVAILABLE, f"unavailable: {type(exc).__name__}"))
            continue
        if predicate(payload):
            results.append(CheckResult(path, SAFE, expected))
        else:
            results.append(CheckResult(path, UNSAFE, f"expected {expected}; got {payload!r}"[:2000]))

    try:
        meta = _fetch_json(origin, "/meta")
    except (HTTPError, URLError, TimeoutError, OSError, RuntimeError, json.JSONDecodeError) as exc:
        results.append(CheckResult("/meta", UNAVAILABLE, f"unavailable: {type(exc).__name__}"))
    else:
        deployed = str(meta.get("revision") or "").strip().lower()
        if deployed == expected_sha:
            results.append(CheckResult("/meta", SAFE, f"revision={deployed}"))
        else:
            results.append(
                CheckResult(
                    "/meta",
                    UNSAFE,
                    f"deployed revision mismatch: expected {expected_sha}, got {deployed or '<missing>'}",
                )
            )
    return results


def _print_results(phase: str, results: list[CheckResult]) -> None:
    payload = {
        "phase": phase,
        "ok": _combine_codes(results) == SAFE,
        "exit_code": _combine_codes(results),
        "checks": [
            {"name": item.name, "code": item.code, "detail": item.detail}
            for item in results
        ],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def run_predeploy() -> tuple[int, list[CheckResult]]:
    results = [_run_script(path) for path in PREDEPLOY_SCRIPTS]
    return _combine_codes(results), results


def run_postdeploy() -> tuple[int, list[CheckResult]]:
    results = _http_contracts()
    for path in POSTDEPLOY_SCRIPTS:
        result = _run_script(path)
        if (
            path.endswith("check_retention_indexes.py")
            and result.code == SAFE
            and "bootstrap_pending" in result.detail
        ):
            result = CheckResult(
                path,
                UNSAFE,
                "post-deploy retention still reports bootstrap_pending",
            )
        results.append(result)
    return _combine_codes(results), results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run read-only production acceptance checks")
    parser.add_argument("phase", choices=("predeploy", "postdeploy"))
    args = parser.parse_args(argv)

    if args.phase == "predeploy":
        code, results = run_predeploy()
    else:
        code, results = run_postdeploy()
    _print_results(args.phase, results)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
