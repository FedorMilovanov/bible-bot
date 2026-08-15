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

    def __init__(self, *, current, open_pulls=None, closed_pulls=None, second_reads=None):
        self.current = dict(current)
        self.open_pulls = list(open_pulls or [])
        self.closed_pulls = list(closed_pulls or [])
        self.second_reads = {key: list(values) for key, values in (second_reads or {}).items()}
        self.deleted = []
        self.read_counts = {}

    def repository_metadata(self):
        return {"default_branch": "main"}

    def pulls(self, state):
        return self.open_pulls if state == "open" else self.closed_pulls

    def ref_sha(self, ref):
        self.read_counts[ref] = self.read_counts.get(ref, 0) + 1
        values = self.second_reads.get(ref)
        if values:
            return values.pop(0)
        return self.current.get(ref)

    def delete_ref(self, ref):
        self.deleted.append(ref)
        self.current.pop(ref, None)


def test_cleanup_deletes_only_ref_still_pinned_to_closed_pr_head():
    api = FakeApi(
        current={
            "agent/stale": "aaa",
            "agent/moved": "new",
            "feature/manual": "ccc",
        },
        closed_pulls=[
            _pull("agent/stale", "aaa"),
            _pull("agent/moved", "old"),
            _pull("feature/manual", "ccc"),
        ],
    )

    deleted, skipped = cleanup(api)

    assert deleted == ["agent/stale"]
    assert api.deleted == ["agent/stale"]
    assert skipped == ["agent/moved: ref moved after closed PR"]


def test_cleanup_never_deletes_branch_with_open_pr():
    api = FakeApi(
        current={"release/work": "aaa"},
        closed_pulls=[_pull("release/work", "aaa")],
        open_pulls=[_pull("release/work", "aaa")],
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["release/work: open PR exists"]


def test_cleanup_rechecks_sha_immediately_before_delete():
    api = FakeApi(
        current={"agent/race": "aaa"},
        closed_pulls=[_pull("agent/race", "aaa")],
        second_reads={"agent/race": ["aaa", "new"]},
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["agent/race: ref moved during cleanup"]
