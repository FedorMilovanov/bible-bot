import json

import pytest

from scripts.cleanup_closed_pr_branches import (
    _load_reviewed_retirements,
    _open_base_refs,
    cleanup,
)


REPO = "FedorMilovanov/bible-bot"
OLD = "a" * 40
REPLACEMENT = "b" * 40
MAIN = "c" * 40


def _retirement(ref="agent/retired", *, tip=OLD, replacement=REPLACEMENT):
    return {
        ref: {
            "retired_tip_sha": tip,
            "replacement_sha": replacement,
            "rationale": "Reviewed historical ref.",
        }
    }


def _open_pull(*, head_ref="agent/child", base_ref="agent/retired", repo=REPO):
    return {
        "head": {"ref": head_ref, "repo": {"full_name": repo}},
        "base": {"ref": base_ref, "repo": {"full_name": repo}},
    }


class FakeApi:
    repository = REPO

    def __init__(
        self,
        *,
        current=None,
        open_pulls=None,
        ancestor_pairs=None,
        second_reads=None,
    ):
        self.current = {"main": MAIN, **dict(current or {"agent/retired": OLD})}
        self.open_pulls = list(open_pulls or [])
        self.ancestor_pairs = set(ancestor_pairs or [])
        self.second_reads = {
            key: list(values) for key, values in (second_reads or {}).items()
        }
        self.deleted = []

    def repository_metadata(self):
        return {"default_branch": "main"}

    def pulls(self, state):
        return self.open_pulls if state == "open" else []

    def branches(self):
        return [{"name": ref} for ref in self.current]

    def ref_sha(self, ref):
        values = self.second_reads.get(ref)
        if values:
            return values.pop(0)
        return self.current.get(ref)

    def is_ancestor(self, base_sha, head_sha):
        return (base_sha, head_sha) in self.ancestor_pairs

    def patch_exactly_present_in(self, branch_sha, default_sha):
        return False

    def delete_ref(self, ref):
        self.deleted.append(ref)
        self.current.pop(ref, None)


def test_open_base_refs_protect_same_repository_stacked_base():
    pulls = [
        _open_pull(),
        _open_pull(base_ref="agent/fork-base", repo="someone/fork"),
    ]
    assert _open_base_refs(pulls, repository=REPO) == {"agent/retired"}


def test_manifest_loader_allows_shared_tip_for_explicit_branches(tmp_path):
    path = tmp_path / "retirements.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "retirements": [
                    {
                        "branch": "agent/one",
                        "retired_tip_sha": OLD,
                        "replacement_sha": REPLACEMENT,
                        "rationale": "one",
                    },
                    {
                        "branch": "agent/two",
                        "retired_tip_sha": OLD,
                        "replacement_sha": REPLACEMENT,
                        "rationale": "two",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    loaded = _load_reviewed_retirements(path)

    assert set(loaded) == {"agent/one", "agent/two"}


@pytest.mark.parametrize("branch", ["agent/*", "agent/name?", "agent/[old]"])
def test_manifest_loader_rejects_non_exact_branch_names(tmp_path, branch):
    path = tmp_path / "retirements.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "retirements": [
                    {
                        "branch": branch,
                        "retired_tip_sha": OLD,
                        "replacement_sha": REPLACEMENT,
                        "rationale": "bad",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="explicit"):
        _load_reviewed_retirements(path)


def test_reviewed_exact_tip_deletes_only_when_replacement_is_in_main():
    api = FakeApi(ancestor_pairs={(REPLACEMENT, MAIN)})

    deleted, skipped = cleanup(api, reviewed_retirements=_retirement())

    assert deleted == ["agent/retired"]
    assert skipped == []


def test_reviewed_retirement_refuses_tip_mismatch():
    api = FakeApi(current={"agent/retired": "d" * 40})

    deleted, skipped = cleanup(api, reviewed_retirements=_retirement())

    assert deleted == []
    assert skipped == ["agent/retired: reviewed retirement SHA mismatch"]


def test_reviewed_retirement_refuses_replacement_not_in_main():
    api = FakeApi()

    deleted, skipped = cleanup(api, reviewed_retirements=_retirement())

    assert deleted == []
    assert skipped == ["agent/retired: reviewed replacement not in main"]


def test_open_pr_base_blocks_reviewed_retirement():
    api = FakeApi(
        open_pulls=[_open_pull()],
        ancestor_pairs={(REPLACEMENT, MAIN)},
    )

    deleted, skipped = cleanup(api, reviewed_retirements=_retirement())

    assert deleted == []
    assert skipped == ["agent/retired: open PR base exists"]


def test_reviewed_retirement_refuses_delete_if_main_moves_after_proof():
    api = FakeApi(
        ancestor_pairs={(REPLACEMENT, MAIN)},
        second_reads={
            "main": [MAIN, "e" * 40],
            "agent/retired": [OLD, OLD],
        },
    )

    deleted, skipped = cleanup(api, reviewed_retirements=_retirement())

    assert deleted == []
    assert skipped == ["agent/retired: main moved during content proof"]
