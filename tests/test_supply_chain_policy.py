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

def test_required_pip_audit_job_includes_dependency_review() -> None:
    workflow = Path(".github/workflows/security-audit.yml").read_text(encoding="utf-8")
    assert "jobs:\\n  pip-audit:" in workflow
    assert "if: github.event_name == 'pull_request'" in workflow
    assert (
        "uses: actions/dependency-review-action@"
        "a1d282b36b6f3519aa1f3fc636f609c47dddb294"
    ) in workflow
    assert "fail-on-severity: moderate" in workflow
    dependency_review = workflow.index("uses: actions/dependency-review-action@")
    pip_audit_install = workflow.index("pip-audit==2.10.1")
    assert dependency_review < pip_audit_install
