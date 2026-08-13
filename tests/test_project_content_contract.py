from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_agent_contract_points_to_live_content_policies():
    agents = _read("AGENTS.md")
    assert "docs/CONTENT_SOURCE_POLICY.md" in agents
    assert "docs/FIRST_PETER_2_5_ROADMAP.md" in agents
    assert (ROOT / "docs/CONTENT_SOURCE_POLICY.md").is_file()
    assert (ROOT / "docs/FIRST_PETER_2_5_ROADMAP.md").is_file()


def test_agent_contract_preserves_content_truth_fields_and_competitive_boundary():
    agents = _read("AGENTS.md")
    for field in ("claim_type", "confidence", "position", "competitive", "sources"):
        assert f"`{field}`" in agents
    assert "SBL Greek New Testament" in agents
    assert "MorphGNT" in agents
    assert "application" in agents
    assert "competitive=false" in agents


def test_content_policy_requires_source_quorum_and_disputed_passage_handling():
    policy = _read("docs/CONTENT_SOURCE_POLICY.md")
    assert "Minimum quorum" in policy
    assert "TMS / John MacArthur" in policy
    assert "Thomas Schreiner" in policy
    assert "Karen Jobes" in policy
    assert "Wayne Grudem" in policy
    assert "1 Pet 3:19-20" in policy
    assert "1 Pet 3:21" in policy
    assert "1 Pet 4:6" in policy


def test_roadmap_forbids_question_count_as_completion_metric():
    roadmap = _read("docs/FIRST_PETER_2_5_ROADMAP.md")
    assert "Question count alone is not sufficient" in roadmap
    for chapter in ("Chapter 2", "Chapter 3", "Chapter 4", "Chapter 5"):
        assert chapter in roadmap
    assert "Per-chapter definition of done" in roadmap
    assert "Whole-book definition of done" in roadmap
