import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW_MODULES = {"questions.chapter1", "questions.intro"}


def _raw_imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in RAW_MODULES:
                    violations.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module in RAW_MODULES:
                violations.append(node.module)
    return violations


def test_production_code_does_not_bypass_canonical_question_package():
    violations = []
    for path in ROOT.rglob("*.py"):
        relative = path.relative_to(ROOT)
        if relative.parts[0] in {"questions", "tests", ".venv", "venv"}:
            continue
        for module in _raw_imports(path):
            violations.append(f"{relative}: {module}")
    assert violations == []


def test_raw_question_files_are_only_imported_at_package_boundary():
    init_text = (ROOT / "questions" / "__init__.py").read_text(encoding="utf-8")
    assert "from .chapter1 import" in init_text
    assert "from .intro import" in init_text
    assert "curate_pool" in init_text
    assert "apply_review_overrides" in init_text
