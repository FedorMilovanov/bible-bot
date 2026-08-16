from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RETIRED_RUNTIME_ARTIFACTS = (
    "bot.py",
    "telegram_controller.py",
    "telegram_controller_legacy_bridge.py",
)
OPERATIONAL_SURFACES = (
    "README.md",
    "ВОССТАНОВЛЕНИЕ.md",
    "Dockerfile",
    "render.yaml",
)
FORBIDDEN_RUNTIME_INSTRUCTIONS = (
    "python bot.py",
    "python telegram_controller.py",
    "import bot",
    "from bot import",
    "import telegram_controller",
    "from telegram_controller import",
)


def test_retired_runtime_artifacts_do_not_exist():
    for relative_path in RETIRED_RUNTIME_ARTIFACTS:
        assert not (ROOT / relative_path).exists(), relative_path


def test_operational_surfaces_cannot_restart_retired_runtime():
    for relative_path in OPERATIONAL_SURFACES:
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        for forbidden in FORBIDDEN_RUNTIME_INSTRUCTIONS:
            assert forbidden not in source, f"{relative_path}: {forbidden}"


def test_deploy_and_recovery_surfaces_name_canonical_entrypoint():
    assert (ROOT / "production_entrypoint.py").is_file()

    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    render = (ROOT / "render.yaml").read_text(encoding="utf-8")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    recovery = (ROOT / "ВОССТАНОВЛЕНИЕ.md").read_text(encoding="utf-8")

    assert 'CMD ["python", "production_entrypoint.py"]' in dockerfile
    assert "startCommand: python production_entrypoint.py" in render
    assert "python production_entrypoint.py" in readme
    assert "python production_entrypoint.py" in recovery
