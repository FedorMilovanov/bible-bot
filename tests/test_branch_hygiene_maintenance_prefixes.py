from scripts.cleanup_closed_pr_branches import _eligible_ref


def test_maintenance_refs_are_service_owned_cleanup_candidates():
    for ref in (
        "hardening/supply-chain-wave",
        "hardening/actions-registry-hygiene",
        "retire/legacy-runtime-monoliths",
        "audit/legacy-file-deletion-rehearsal",
    ):
        assert _eligible_ref(ref, default_branch="main") is True


def test_manual_and_default_refs_remain_ineligible():
    assert _eligible_ref("feature/user-work", default_branch="main") is False
    assert _eligible_ref("arena/old", default_branch="main") is False
    assert _eligible_ref("main", default_branch="main") is False
