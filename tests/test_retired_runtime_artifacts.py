from __future__ import annotations

import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RETIRED_RUNTIME_ROOTS = {
    "bot",
    "telegram_controller",
    "telegram_controller_legacy_bridge",
}
RETIRED_RUNTIME_ARTIFACTS = tuple(f"{name}.py" for name in sorted(RETIRED_RUNTIME_ROOTS))
OPERATIONAL_SURFACES = (
    "README.md",
    "ВОССТАНОВЛЕНИЕ.md",
    "Dockerfile",
    "render.yaml",
)
IGNORED_SCAN_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "node_modules",
}
TEXT_SUFFIXES = {
    ".cfg",
    ".cmd",
    ".conf",
    ".ini",
    ".json",
    ".md",
    ".ps1",
    ".sh",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}
RETIRED_LAUNCH_PATTERNS = (
    re.compile(
        r"\bpython(?:3(?:\.\d+)?)?(?:\s+-\S+)*\s+(?:\./)?"
        r"(?:bot|telegram_controller|telegram_controller_legacy_bridge)\.py\b"
    ),
    re.compile(
        r"\bpython(?:3(?:\.\d+)?)?(?:\s+-\S+)*\s+-m\s+"
        r"(?:bot|telegram_controller|telegram_controller_legacy_bridge)\b"
    ),
    re.compile(
        r"\b(?:bot|telegram_controller|telegram_controller_legacy_bridge):"
        r"(?:app|application|main)\b"
    ),
)


def _is_ignored(path: Path) -> bool:
    relative = path.relative_to(ROOT)
    return any(part in IGNORED_SCAN_PARTS for part in relative.parts)


def _python_files() -> list[Path]:
    return sorted(path for path in ROOT.rglob("*.py") if not _is_ignored(path))


def _operational_text_files() -> list[Path]:
    files = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or _is_ignored(path):
            continue
        relative = path.relative_to(ROOT)
        if relative.parts and relative.parts[0] == "tests":
            continue
        if path.name == "Dockerfile" or path.suffix.lower() in TEXT_SUFFIXES:
            files.append(path)
    return sorted(files)


def _root_module(name: str | None) -> str | None:
    if not name:
        return None
    return name.split(".", 1)[0]


def _literal_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def test_retired_runtime_artifacts_do_not_exist():
    for relative_path in RETIRED_RUNTIME_ARTIFACTS:
        assert not (ROOT / relative_path).exists(), relative_path


def test_no_python_file_can_import_retired_runtime_roots():
    violations = []
    for path in _python_files():
        relative = path.relative_to(ROOT)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(relative))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root = _root_module(alias.name)
                    if root in RETIRED_RUNTIME_ROOTS:
                        violations.append(f"{relative}:{node.lineno}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                root = _root_module(node.module)
                if root in RETIRED_RUNTIME_ROOTS:
                    violations.append(f"{relative}:{node.lineno}: from {node.module} import ...")
            elif isinstance(node, ast.Call):
                dynamic_name = None
                if (
                    isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "importlib"
                    and node.func.attr == "import_module"
                ):
                    dynamic_name = _literal_string(node.args[0]) if node.args else None
                elif isinstance(node.func, ast.Name) and node.func.id == "__import__":
                    dynamic_name = _literal_string(node.args[0]) if node.args else None

                root = _root_module(dynamic_name)
                if root in RETIRED_RUNTIME_ROOTS:
                    violations.append(
                        f"{relative}:{node.lineno}: dynamic import {dynamic_name!r}"
                    )

    assert violations == []


def test_no_python_file_defines_a_monolith_bridge_installer():
    violations = []
    for path in _python_files():
        relative = path.relative_to(ROOT)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(relative))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "install_legacy_bridge":
                violations.append(f"{relative}:{node.lineno}: {node.name}")

    assert violations == []


def test_repository_operational_text_cannot_launch_retired_runtime():
    violations = []
    for path in _operational_text_files():
        relative = path.relative_to(ROOT)
        source = path.read_text(encoding="utf-8", errors="replace")
        for pattern in RETIRED_LAUNCH_PATTERNS:
            for match in pattern.finditer(source):
                line = source.count("\n", 0, match.start()) + 1
                violations.append(f"{relative}:{line}: {match.group(0)}")

    assert violations == []


def test_named_operational_surfaces_cannot_reintroduce_retired_imports():
    forbidden = (
        "import bot",
        "from bot import",
        "import telegram_controller",
        "from telegram_controller import",
        "import telegram_controller_legacy_bridge",
        "from telegram_controller_legacy_bridge import",
    )
    for relative_path in OPERATIONAL_SURFACES:
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in source, f"{relative_path}: {token}"


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
