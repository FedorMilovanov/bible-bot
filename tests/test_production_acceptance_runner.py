from pathlib import Path

import pytest

from scripts import run_production_acceptance as acceptance


ROOT = Path(__file__).resolve().parents[1]
ACCEPTANCE_DOC = (ROOT / "docs" / "PRODUCTION_ACCEPTANCE.md").read_text(
    encoding="utf-8"
)


def test_combine_codes_prefers_unavailable_over_unsafe():
    results = [
        acceptance.CheckResult("safe", acceptance.SAFE, "ok"),
        acceptance.CheckResult("unsafe", acceptance.UNSAFE, "bad"),
        acceptance.CheckResult("unavailable", acceptance.UNAVAILABLE, "down"),
    ]
    assert acceptance._combine_codes(results) == acceptance.UNAVAILABLE


def test_deployment_origin_requires_https_origin(monkeypatch):
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bible-bot.onrender.com")
    monkeypatch.delenv("TELEGRAM_WEBHOOK_BASE_URL", raising=False)
    assert acceptance._deployment_origin() == "https://bible-bot.onrender.com"

    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bible-bot.onrender.com/path")
    with pytest.raises(ValueError, match="HTTPS origin"):
        acceptance._deployment_origin()


def test_expected_deploy_sha_is_exact_40_hex(monkeypatch):
    sha = "a" * 40
    monkeypatch.setenv("EXPECTED_DEPLOY_SHA", sha.upper())
    assert acceptance._expected_deploy_sha() == sha

    monkeypatch.setenv("EXPECTED_DEPLOY_SHA", "abc")
    with pytest.raises(ValueError, match="40-hex"):
        acceptance._expected_deploy_sha()


def test_http_contracts_require_exact_ready_and_revision(monkeypatch):
    sha = "b" * 40
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://example.onrender.com")
    monkeypatch.setenv("EXPECTED_DEPLOY_SHA", sha)

    payloads = {
        "/live": {"status": "ok", "uptime_seconds": 1},
        "/ready": {"status": "ready", "database": True},
        "/telegram/ready": {"status": "ready", "transport": "webhook"},
        "/meta": {"revision": sha},
    }
    monkeypatch.setattr(
        acceptance,
        "_fetch_json",
        lambda _origin, path: payloads[path],
    )

    results = acceptance._http_contracts()
    assert acceptance._combine_codes(results) == acceptance.SAFE
    assert {item.name for item in results} == {
        "/live",
        "/ready",
        "/telegram/ready",
        "/meta",
    }


def test_http_contracts_reject_old_deployed_revision(monkeypatch):
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://example.onrender.com")
    monkeypatch.setenv("EXPECTED_DEPLOY_SHA", "c" * 40)

    def payload(_origin, path):
        if path == "/live":
            return {"status": "ok"}
        if path == "/ready":
            return {"status": "ready", "database": True}
        if path == "/telegram/ready":
            return {"status": "ready", "transport": "webhook"}
        return {"revision": "d" * 40}

    monkeypatch.setattr(acceptance, "_fetch_json", payload)
    results = acceptance._http_contracts()
    meta = next(item for item in results if item.name == "/meta")
    assert meta.code == acceptance.UNSAFE
    assert "revision mismatch" in meta.detail


def test_postdeploy_rejects_retention_bootstrap_pending(monkeypatch):
    monkeypatch.setattr(
        acceptance,
        "_http_contracts",
        lambda: [acceptance.CheckResult("http", acceptance.SAFE, "ok")],
    )

    def run_script(path):
        if path.endswith("check_retention_indexes.py"):
            return acceptance.CheckResult(
                path,
                acceptance.SAFE,
                '{"ok": true, "bootstrap_pending": ["x"]}',
            )
        return acceptance.CheckResult(path, acceptance.SAFE, "SAFE")

    monkeypatch.setattr(acceptance, "_run_script", run_script)
    code, results = acceptance.run_postdeploy()
    assert code == acceptance.UNSAFE
    retention = next(
        item for item in results if item.name.endswith("check_retention_indexes.py")
    )
    assert retention.code == acceptance.UNSAFE
    assert "bootstrap_pending" in retention.detail


def test_run_predeploy_executes_all_five_read_only_checks(monkeypatch):
    seen = []

    def run_script(path):
        seen.append(path)
        return acceptance.CheckResult(path, acceptance.SAFE, "ok")

    monkeypatch.setattr(acceptance, "_run_script", run_script)
    code, _results = acceptance.run_predeploy()
    assert code == acceptance.SAFE
    assert tuple(seen) == acceptance.PREDEPLOY_SCRIPTS


def test_acceptance_document_keeps_both_phases_and_exact_revision_gate():
    required = (
        "run_production_acceptance.py predeploy",
        "run_production_acceptance.py postdeploy",
        "EXPECTED_DEPLOY_SHA",
        "/telegram/ready",
        "100% accepted",
    )
    for marker in required:
        assert marker in ACCEPTANCE_DOC
