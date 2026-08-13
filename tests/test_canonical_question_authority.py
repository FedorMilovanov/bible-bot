from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_production_code_does_not_bypass_canonical_question_package():
    violations = []
    for path in ROOT.rglob("*.py"):
        relative = path.relative_to(ROOT)
        if relative.parts[0] in {"questions", "tests", ".venv", "venv"}:
            continue
        text = path.read_text(encoding="utf-8")
        if "questions.chapter1" in text or "questions.intro" in text:
            violations.append(str(relative))
    assert violations == []


def test_raw_question_files_are_only_imported_at_package_boundary():
    init_text = (ROOT / "questions" / "__init__.py").read_text(encoding="utf-8")
    assert "from .chapter1 import" in init_text
    assert "from .intro import" in init_text
    assert "curate_pool" in init_text
    assert "apply_review_overrides" in init_text
