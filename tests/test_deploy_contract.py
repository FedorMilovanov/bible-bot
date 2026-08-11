from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PYTHON_VERSION = "3.11.15"
PRODUCTION_CONTROLLER = "telegram_production.py"


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_python_version_is_consistent_across_runtime_surfaces():
    assert read(".python-version").strip() == PYTHON_VERSION
    assert f"python-version: '{PYTHON_VERSION}'" in read(".github/workflows/ci.yml")
    assert f"python-version: '{PYTHON_VERSION}'" in read(".github/workflows/security-audit.yml")
    assert f"FROM python:{PYTHON_VERSION}-slim-trixie@sha256:" in read("Dockerfile")


def test_single_process_start_contract_is_consistent():
    render = read("render.yaml")
    docker = read("Dockerfile")
    assert f"startCommand: python {PRODUCTION_CONTROLLER}" in render
    assert f'CMD ["python", "{PRODUCTION_CONTROLLER}"]' in docker
    assert "startCommand: python bot.py" not in render
    assert 'CMD ["python", "bot.py"]' not in docker
    assert "startCommand: python telegram_controller.py" not in render
    assert 'CMD ["python", "telegram_controller.py"]' not in docker
    assert "healthCheckPath: /live" in render


def test_readme_documents_same_production_entrypoint_and_preflight_runbook():
    readme = read("README.md")
    assert "python telegram_production.py" in readme
    assert "python bot.py" not in readme
    assert "docs/DEPLOYMENT_PREFLIGHTS.md" in readme
    assert "telegram_production.py` — единственный production Telegram composition root" in readme


def test_render_uses_current_runtime_field_and_resource_bounds():
    render = read("render.yaml")
    assert "runtime: python" in render
    assert "MAX_REQUEST_BODY_BYTES" in render
    assert "MAX_REQUEST_HEADER_BYTES" in render
    assert "healthCheckPath: /live" in render


def test_docker_runtime_is_non_root_and_has_healthcheck():
    docker = read("Dockerfile")
    assert "useradd --uid 10001 --gid 10001" in docker
    assert "COPY --chown=10001:10001 . ." in docker
    assert "USER 10001:10001" in docker
    assert "HEALTHCHECK" in docker
    assert "/live" in docker


def test_docker_context_excludes_local_secrets_and_dev_tree():
    ignored = {
        line.strip()
        for line in read(".dockerignore").splitlines()
        if line.strip() and not line.startswith("#")
    }
    assert ".env" in ignored
    assert ".env.*" in ignored
    assert ".git" in ignored
    assert ".github" in ignored
    assert "tests" in ignored
    assert "docs" in ignored


def test_production_requires_ptb_job_queue_for_recovery_jobs():
    requirements = read("requirements.txt")
    source = read("telegram_production.py")
    assert "python-telegram-bot[job-queue]==20.7" in requirements
    assert "if app.job_queue is None:" in source
    assert "if app.job_queue is not None:" not in source
    assert "reports.report_delivery_job" in source
    assert "battles.battle_maintenance_job" in source


def test_legacy_render_worker_config_is_gone():
    assert not (ROOT / "render.yaml.txt").exists()
