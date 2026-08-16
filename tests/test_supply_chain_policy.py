from pathlib import Path

import pytest

from scripts.check_supply_chain_policy import (
    _parse_pinned_requirements,
    _workflow_write_permission_violations,
    collect_violations,
)


def test_repository_supply_chain_policy_is_closed() -> None:
    assert collect_violations() == []


def test_requirement_parser_rejects_non_exact_pin(tmp_path: Path) -> None:
    path = tmp_path / "requirements.txt"
    path.write_text("example>=1.2\n", encoding="utf-8")

    with pytest.raises(ValueError, match="exact == pin"):
        _parse_pinned_requirements(path)


def test_requirement_parser_normalizes_extras_name(tmp_path: Path) -> None:
    path = tmp_path / "requirements.txt"
    path.write_text("Example_Pkg[extra]==1.2.3\n", encoding="utf-8")

    assert _parse_pinned_requirements(path) == {"example-pkg": "1.2.3"}


def test_only_branch_hygiene_gets_actions_and_contents_write() -> None:
    workflow = Path("branch-hygiene.yml")
    assert _workflow_write_permission_violations(
        workflow,
        ["permissions:", "  actions: write", "  contents: write", "  pull-requests: read"],
    ) == []


def test_unexpected_workflow_write_permission_is_rejected() -> None:
    violations = _workflow_write_permission_violations(
        Path("ci.yml"),
        ["permissions:", "  contents: write"],
    )
    assert len(violations) == 1
    assert "not allowlisted" in violations[0]


def test_codeql_security_events_write_remains_allowlisted() -> None:
    assert _workflow_write_permission_violations(
        Path("codeql.yml"),
        ["permissions:", "  contents: read", "  security-events: write"],
    ) == []


def test_write_all_is_always_rejected() -> None:
    violations = _workflow_write_permission_violations(
        Path("branch-hygiene.yml"),
        ["permissions: write-all"],
    )
    assert len(violations) == 1
    assert "write-all is forbidden" in violations[0]
