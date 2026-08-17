from scripts.cleanup_closed_pr_branches import (
    GitHubApi,
    _eligible_ref,
    _merged_default_head_shas,
    _merged_default_merge_shas,
    _open_head_refs,
    cleanup,
)


REPO = "FedorMilovanov/bible-bot"


def _pull(
    ref,
    sha,
    *,
    head_repo=REPO,
    base_repo=REPO,
    base_ref="main",
    merged=True,
    merge_sha="main-sha",
):
    return {
        "merged_at": "2026-08-15T00:00:00Z" if merged else None,
        "merge_commit_sha": merge_sha if merged else None,
        "head": {
            "ref": ref,
            "sha": sha,
            "repo": {"full_name": head_repo} if head_repo else None,
        },
        "base": {
            "ref": base_ref,
            "repo": {"full_name": base_repo} if base_repo else None,
        },
    }


def test_only_service_owned_work_prefixes_are_eligible():
    assert _eligible_ref("agent/fix", default_branch="main") is True
    assert _eligible_ref("release/runtime", default_branch="main") is True
    assert _eligible_ref("dependabot/pip/pytest", default_branch="main") is True
    assert _eligible_ref("feature/user-work", default_branch="main") is False
    assert _eligible_ref("arena/old", default_branch="main") is False
    assert _eligible_ref("main", default_branch="main") is False


def test_merged_head_map_requires_same_repo_default_branch_merge():
    pulls = [
        _pull("agent/good", "aaa"),
        _pull("agent/closed-only", "bbb", merged=False),
        _pull("agent/staging", "ccc", base_ref="staging"),
        _pull("agent/fork-head", "ddd", head_repo="someone/fork"),
        _pull("agent/fork-base", "eee", base_repo="someone/fork"),
        _pull("feature/manual", "fff"),
    ]

    result = _merged_default_head_shas(
        pulls,
        repository=REPO,
        default_branch="main",
    )

    assert result == {"agent/good": {"aaa"}}


def test_merged_merge_map_pins_head_to_merge_commit():
    pulls = [
        _pull("agent/good", "aaa", merge_sha="merge-aaa"),
        _pull("agent/no-merge-sha", "bbb", merge_sha=""),
        _pull("agent/staging", "ccc", base_ref="staging", merge_sha="merge-ccc"),
        _pull("agent/fork", "ddd", head_repo="someone/fork", merge_sha="merge-ddd"),
    ]

    assert _merged_default_merge_shas(
        pulls,
        repository=REPO,
        default_branch="main",
    ) == {("agent/good", "aaa"): {"merge-aaa"}}


def test_open_refs_only_include_same_repository_heads():
    pulls = [
        _pull("agent/open", "aaa"),
        _pull("agent/fork", "bbb", head_repo="someone/fork"),
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


class PatchProofApi:
    def __init__(self, files, contents):
        self.files = files
        self.contents = contents

    def _compare(self, base_sha, head_sha):
        assert base_sha == "main-sha"
        assert head_sha == "branch-sha"
        return {"files": self.files}

    def content_sha(self, path, ref_sha):
        assert ref_sha == "main-sha"
        return self.contents.get(path)


def _patch_proof(files, contents):
    api = PatchProofApi(files, contents)
    return GitHubApi.patch_exactly_present_in(api, "branch-sha", "main-sha")


def test_exact_patch_proof_accepts_matching_modified_added_and_copied_blobs():
    files = [
        {"filename": "a.py", "status": "modified", "sha": "a1"},
        {"filename": "b.py", "status": "added", "sha": "b1"},
        {"filename": "c.py", "status": "copied", "sha": "c1"},
    ]
    assert _patch_proof(files, {"a.py": "a1", "b.py": "b1", "c.py": "c1"})


def test_exact_patch_proof_rejects_one_mismatched_blob():
    files = [{"filename": "a.py", "status": "modified", "sha": "branch"}]
    assert not _patch_proof(files, {"a.py": "main"})


def test_exact_patch_proof_accepts_removal_only_when_path_is_absent_in_main():
    files = [{"filename": "old.py", "status": "removed", "sha": "old"}]
    assert _patch_proof(files, {})
    assert not _patch_proof(files, {"old.py": "still-present"})


def test_exact_patch_proof_requires_both_sides_of_rename():
    files = [
        {
            "filename": "new.py",
            "previous_filename": "old.py",
            "status": "renamed",
            "sha": "new-blob",
        }
    ]
    assert _patch_proof(files, {"new.py": "new-blob"})
    assert not _patch_proof(
        files,
        {"new.py": "new-blob", "old.py": "old-still-present"},
    )
    assert not _patch_proof(files, {"new.py": "different"})


def test_exact_patch_proof_fails_closed_at_compare_file_cap():
    files = [
        {"filename": f"f{i}.py", "status": "modified", "sha": f"s{i}"}
        for i in range(300)
    ]
    contents = {f"f{i}.py": f"s{i}" for i in range(300)}
    assert not _patch_proof(files, contents)


def test_exact_patch_proof_rejects_unknown_status():
    files = [{"filename": "a.py", "status": "mystery", "sha": "a1"}]
    assert not _patch_proof(files, {"a.py": "a1"})


def test_cleanup_deletes_exact_head_of_pr_merged_into_main():
    api = FakeApi(
        current={"agent/merged": "aaa"},
        closed_pulls=[_pull("agent/merged", "aaa")],
    )

    deleted, skipped = cleanup(api)

    assert deleted == ["agent/merged"]
    assert skipped == []


def test_cleanup_does_not_trust_merged_head_if_merge_commit_left_current_main():
    api = FakeApi(
        current={"agent/merged": "aaa"},
        closed_pulls=[_pull("agent/merged", "aaa", merge_sha="retired-main")],
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["agent/merged: unique content not proven in main"]


def test_closed_but_unmerged_pr_is_not_deletion_authority():
    api = FakeApi(
        current={"agent/closed-only": "aaa"},
        closed_pulls=[_pull("agent/closed-only", "aaa", merged=False)],
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["agent/closed-only: unique content not proven in main"]


def test_pr_merged_into_non_default_branch_is_not_deletion_authority():
    api = FakeApi(
        current={"agent/staging": "aaa"},
        closed_pulls=[_pull("agent/staging", "aaa", base_ref="staging")],
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["agent/staging: unique content not proven in main"]


def test_closed_unmerged_branch_can_still_delete_when_ancestor_of_main():
    api = FakeApi(
        current={"agent/closed-ancestor": "aaa"},
        closed_pulls=[_pull("agent/closed-ancestor", "aaa", merged=False)],
        ancestors={"aaa"},
    )

    deleted, skipped = cleanup(api)

    assert deleted == ["agent/closed-ancestor"]
    assert skipped == []


def test_closed_unmerged_branch_can_still_delete_when_patch_is_exactly_in_main():
    api = FakeApi(
        current={"agent/closed-cherry-pick": "aaa"},
        closed_pulls=[_pull("agent/closed-cherry-pick", "aaa", merged=False)],
        exact_content={"aaa"},
    )

    deleted, skipped = cleanup(api)

    assert deleted == ["agent/closed-cherry-pick"]
    assert skipped == []


def test_cleanup_ignores_non_service_historical_refs():
    api = FakeApi(
        current={"arena/old-audit-ref": "aaa", "feature/manual": "bbb"},
        ancestors={"aaa", "bbb"},
        exact_content={"aaa", "bbb"},
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert skipped == []
    assert api.deleted == []


def test_cleanup_deletes_service_branch_without_pr_when_commit_is_in_main():
    api = FakeApi(
        current={"agent/already-merged": "aaa"},
        ancestors={"aaa"},
    )

    deleted, skipped = cleanup(api)

    assert deleted == ["agent/already-merged"]
    assert skipped == []


def test_cleanup_deletes_divergent_branch_when_patch_is_byte_identical_in_main():
    api = FakeApi(
        current={"agent/cherry-picked": "aaa"},
        exact_content={"aaa"},
    )

    deleted, skipped = cleanup(api)

    assert deleted == ["agent/cherry-picked"]
    assert skipped == []


def test_cleanup_deletes_moved_merged_pr_branch_only_when_new_head_is_in_main():
    api = FakeApi(
        current={"agent/moved": "new"},
        closed_pulls=[_pull("agent/moved", "old")],
        ancestors={"new"},
    )

    deleted, skipped = cleanup(api)

    assert deleted == ["agent/moved"]
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
        open_pulls=[_pull("release/work", "aaa", merged=False)],
        ancestors={"aaa"},
        exact_content={"aaa"},
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["release/work: open PR exists"]


def test_cleanup_rechecks_sha_immediately_before_merged_pr_delete():
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


def test_cleanup_refuses_merged_pr_delete_if_main_moves_after_proof():
    api = FakeApi(
        current={"agent/merged": "aaa"},
        closed_pulls=[_pull("agent/merged", "aaa")],
        second_reads={
            "main": ["main-sha", "new-main"],
            "agent/merged": ["aaa", "aaa"],
        },
    )

    deleted, skipped = cleanup(api)

    assert deleted == []
    assert api.deleted == []
    assert skipped == ["agent/merged: main moved during content proof"]


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
