from pathlib import Path


WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "branch-hygiene.yml"


def test_branch_hygiene_serializes_destructive_sweeps():
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "concurrency:\n" in text
    assert "  group: branch-hygiene-${{ github.repository }}\n" in text
    assert "  cancel-in-progress: false\n" in text
