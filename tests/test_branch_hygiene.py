from scripts.cleanup_closed_pr_branches import (
    _closed_head_shas,
    _eligible_ref,
    _open_head_refs,
    cleanup,
)


REPO = "FedorMilovanov/bible-bot"


def _pull(ref, sha, *, repo=REPO):
    return {
        "head": {
            "ref": ref,
            "sha": sha,
            "repo": {"full_name": repo} if repo else None,
        }
    }


def test_only_service_owned_work_prefixes_are_eligible():
    assert _eligible_ref("agent/fix", default_branch="main") is True
    assert _eligible_ref("release/runtime", default_branch="main") is True
    assert _eligible_ref("dependabot/pip/pytest", default_branch="main") is True
    assert _eligible_ref("feature/user-work", default_branch="main") is False
    assert _eligible_ref("main", default_branch="main") is False


def test_closed_head_map_ignores_forks_and_non_service_branches():
    pulls = [
        _pull("agent/a", "aaa"),
        _pull("agent/a", "bbb"),
        _pull("feature/manual", "ccc"),
        _pull("release/fork", "ddd", repo="someone/fork"),
    ]

    result = _closed_head_shas(
        pulls,
        repository=REPO,
        default_branch="main",
    )

    assert result == {"agent/a": {"aaa", "bbb"}}


def test_open_refs_only_include_same_repository_heads():
    pulls = [
        _pull("agent/open", "aaa"),
        _pull("agent/fork", "bbb", repo="someone/fork"),
    ]

    assert _open_head_refs(pulls, repository=REPO) == {"agent/open"}


class FakeApi:
    repository = REPO

    def __init__(
        self,
        *,
        current,
        open_pulls=None,
        closed_pulls=None,
        ancestors=None,
        exact_content=None,
        second_reads=None,
    ):
        self.current = {"main": "main-sha", **dict(current)}
        self.open_pulls = list(open_pulls or [])
        self.closed_pulls = list(closed_pulls or [])
        self.ancestors = set(ancestors or [])
        self.exact_content = set(exact_content or [])
        self.second_reads = {
            key: list(values) for key, values in (second_reads or {}).items()
        }
        self.deleted = []
        self.read_counts = {}

    def repository_metadata(self):
        return {"default_branch": "main"}

    def pulls(self, state):
        return self.open_pulls if state == "open" else self.closed_pulls

    def branches(self):
        return [{"name": ref} for ref in self.current]

    def ref_sha(self, ref):
        self.read_counts[ref] = self.read_counts.get(ref, 0) + 1
        values = self.second_reads.get(ref)
        if values:
            return values.pop(0)
        return self.current.get(ref)

    def is_ancestor(self, base_sha, head_sha):
        assert head_sha == "main-sha"
        return base_sha in self.ancestors

    def patch_exactly_present_in(self, branch_sha, default_sha):
        assert default_sha == "main-sha"
        return branch_sha in self.exact_content

    def delete_ref(self, ref):
        self.deleted.append(ref)
        self.current.pop(ref, None)


def test_cleanup_deletes_ref_still_pinned_to_closed_pr_head():
    api = FakeApi(
        current={
            "agent/stale": "aaa",
            "feature/manual": "ccc",
        },
        closed_pulls=[
            _pull("agent/stale", "aaa"),
            _pull("feature/manual", "ccc"),
        ],
    )

    deleted, skipped = cleanup(api)

    assert deleted == ["agent/stale"]
    assert api.deleted == ["agent/stale"]
    assert skipped == []


def test_cleanup_deletes_service_branch_without_pr_when_commit_is_in_main():
    api = FakeApi(
        current={"agent/already-merged": "aaa"},
        ancestors={"aaa"},
    )

    deleted, skipped = cleanup(api)

    assert deleted == ["agent/already-merged"]
    assert api.deleted == ["agent/already-merged"]
    assert skipped == []


def test_cleanup_deletes_divergent_branch_when_patch_is_byte_identical_in_main():
    api = FakeApi(
        current={"agent/cherry-picked": "aaa"},
        exact_content={"aaa"},
    )

    deleted, skipped = cleanup(api)

    assert deleted == ["agent/cherry-picked"]
    assert api.deleted == ["agent/cherry-picked"]
    assert skipped == []


def test_cleanup_deletes_moved_closed_pr_branch_only_when_new_head_is_in_main():
    api = FakeApi(
        current={"agent/moved": "new"},
        closed_pulls=[_pull("agent/moved", "old")],
        ancestors={"new"},
    )

    deleted, skipped = cleanup(api)

    assert deleted == ["agent/moved"]
    assert api.deleted == ["agent/moved"]
    assert skipped == []


def test_cleanup_retains_unique_unmatched_service_content():
    api = FakeApi(current={"agent/unique": "aaa"})

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["agent/unique: unique content not proven in main"]


def test_cleanup_never_deletes_branch_with_open_pr():
    api = FakeApi(
        current={"release/work": "aaa"},
        closed_pulls=[_pull("release/work", "aaa")],
        open_pulls=[_pull("release/work", "aaa")],
        ancestors={"aaa"},
        exact_content={"aaa"},
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["release/work: open PR exists"]


def test_cleanup_rechecks_sha_immediately_before_closed_pr_delete():
    api = FakeApi(
        current={"agent/race": "aaa"},
        closed_pulls=[_pull("agent/race", "aaa")],
        second_reads={"agent/race": ["aaa", "new"]},
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["agent/race: ref moved during cleanup"]


def test_cleanup_rechecks_sha_immediately_before_ancestor_delete():
    api = FakeApi(
        current={"agent/race": "aaa"},
        ancestors={"aaa"},
        second_reads={"agent/race": ["aaa", "new"]},
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["agent/race: ref moved during cleanup"]


def test_cleanup_rechecks_sha_immediately_before_exact_content_delete():
    api = FakeApi(
        current={"agent/race": "aaa"},
        exact_content={"aaa"},
        second_reads={"agent/race": ["aaa", "new"]},
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["agent/race: ref moved during cleanup"]


def test_cleanup_refuses_ancestor_delete_if_main_moves_after_proof():
    api = FakeApi(
        current={"agent/merged": "aaa"},
        ancestors={"aaa"},
        second_reads={
            "main": ["main-sha", "new-main"],
            "agent/merged": ["aaa", "aaa"],
        },
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["agent/merged: main moved during content proof"]


def test_cleanup_refuses_exact_content_delete_if_main_moves_after_proof():
    api = FakeApi(
        current={"agent/cherry-picked": "aaa"},
        exact_content={"aaa"},
        second_reads={
            "main": ["main-sha", "new-main"],
            "agent/cherry-picked": ["aaa", "aaa"],
        },
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["agent/cherry-picked: main moved during content proof"]
