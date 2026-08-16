from __future__ import annotations

import ast
from collections import deque
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_PRODUCTION_MODULES = {"bot", "telegram_controller"}


def _module_path(module: str) -> Path | None:
    parts = module.split(".")
    file_candidate = ROOT.joinpath(*parts).with_suffix(".py")
    if file_candidate.is_file():
        return file_candidate
    package_candidate = ROOT.joinpath(*parts, "__init__.py")
    if package_candidate.is_file():
        return package_candidate
    return None


def _resolve_local_module(name: str) -> str | None:
    parts = name.split(".")
    for end in range(len(parts), 0, -1):
        candidate = ".".join(parts[:end])
        if _module_path(candidate) is not None:
            return candidate
    return None


class _ModuleLoadImportVisitor(ast.NodeVisitor):
    """Collect imports executed while a module loads, not dormant function bodies."""

    def __init__(self) -> None:
        self.imports: set[str] = set()

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            candidate = _resolve_local_module(alias.name)
            if candidate is not None:
                self.imports.add(candidate)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level == 0 and node.module:
            candidate = _resolve_local_module(node.module)
            if candidate is not None:
                self.imports.add(candidate)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        del node

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        del node

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        # Bases/decorators may run at import time, but production local modules do
        # not use imports in those expressions. Skipping the body is the critical
        # distinction from ast.walk: method-local compatibility imports are dormant.
        for base in node.bases:
            self.visit(base)
        for keyword in node.keywords:
            self.visit(keyword.value)
        for decorator in node.decorator_list:
            self.visit(decorator)


def _local_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    visitor = _ModuleLoadImportVisitor()
    visitor.visit(tree)
    return visitor.imports


def _reachable_import_paths(entry: str) -> dict[str, tuple[str, ...]]:
    paths: dict[str, tuple[str, ...]] = {entry: (entry,)}
    pending = deque([entry])
    while pending:
        module = pending.popleft()
        path = _module_path(module)
        assert path is not None, f"local module could not be resolved: {module}"
        for dependency in sorted(_local_imports(path)):
            if dependency in paths:
                continue
            paths[dependency] = (*paths[module], dependency)
            pending.append(dependency)
    return paths


def test_deployed_module_load_graph_has_no_bot_or_giant_controller():
    paths = _reachable_import_paths("production_entrypoint")
    violations = {
        module: " -> ".join(paths[module])
        for module in sorted(FORBIDDEN_PRODUCTION_MODULES & paths.keys())
    }
    assert violations == {}
    assert "telegram_production" in paths
    assert "telegram_quiz_runtime_controller" in paths


def test_production_root_has_no_legacy_bootstrap_spelling():
    source = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
    assert 'importlib.import_module("bot")' not in source
    assert "import telegram_controller" not in source
    assert "from telegram_controller" not in source
    assert "legacy =" not in source
    assert "install_legacy_bridge(legacy" not in source
    assert "import telegram_quiz_runtime_controller as quiz" in source


def test_canonical_quiz_runtime_has_no_legacy_controller_import():
    source = (ROOT / "telegram_quiz_runtime_controller.py").read_text(encoding="utf-8")
    assert "import bot" not in source
    assert "from bot" not in source
    assert "import telegram_controller" not in source
    assert "from telegram_controller" not in source
    assert "legacy." not in source


def test_old_utils_gc_is_not_registered_by_production():
    source = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
    assert "from utils import cleanup_stale_userdata" not in source
    assert "utils.cleanup_stale_userdata(" not in source
    assert "import telegram_runtime_maintenance as maintenance" in source
    assert "maintenance.cleanup_stale_userdata_job(context)" in source
