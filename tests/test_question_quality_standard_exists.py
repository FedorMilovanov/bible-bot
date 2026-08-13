from pathlib import Path


def test_question_quality_standard_exists():
    root = Path(__file__).resolve().parents[1]
    assert (root / "docs/QUESTION_QUALITY_STANDARD.md").is_file()
