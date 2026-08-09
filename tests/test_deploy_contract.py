from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PYTHON_VERSION = "3.11.15"


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
    assert "startCommand: python bot.py" in render
    assert 'CMD ["python", "bot.py"]' in docker
    assert "healthCheckPath: /live" in render


def test_render_uses_current_runtime_field_and_resource_bounds():
    render = read("render.yaml")
    assert "runtime: python" in render
    assert "MAX_REQUEST_BODY_BYTES" in render
    assert "MAX_REQUEST_HEADER_BYTES" in render
    assert "healthCheckPath: /live" in render


def test_legacy_render_worker_config_is_gone():
    assert not (ROOT / "render.yaml.txt").exists()
