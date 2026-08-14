import json
from pathlib import Path

import questions
from questions.chapter3.challenge_taxonomy import CHAPTER3_CHALLENGE_TAXONOMY
from questions.chapter3.ranking_audit import (
    CHAPTER3_RANKING_AUDIT,
    CHAPTER3_RANKING_HOLD_REASONS,
    CHAPTER3_RANKING_READY_IDS,
    CLAIM_READY_SOURCE_STATUSES,
    LIMITED_SOURCE_STATUSES,
)
from questions.chapter3.ranking_authority import CHAPTER3_RANKING_AUTHORIZED_IDS
from questions.chapter3.reviewed import (
    CHAPTER3_RANKING_CANDIDATE_IDS,
    CHAPTER3_REVIEWED_QUESTIONS,
)
from questions.ranking_policy import ranking_eligible

MANIFEST_PATH = Path(__file__).resolve().parents[1] / "data" / "chapter3-ranking-audit-manifest.json"
MANIFEST = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _ids(items):
    return {str(item["id"]) for item in items}


def test_audit_partitions_every_reviewed_competitive_candidate():
    assert set(CHAPTER3_RANKING_AUDIT) == set(CHAPTER3_RANKING_CANDIDATE_IDS)
    assert CHAPTER3_RANKING_READY_IDS.isdisjoint(CHAPTER3_RANKING_HOLD_REASONS)
    assert CHAPTER3_RANKING_READY_IDS | set(CHAPTER3_RANKING_HOLD_REASONS) == set(CHAPTER3_RANKING_CANDIDATE_IDS)


def test_ready_ids_pass_existing_structural_policy_and_are_objective_text():
    reviewed = {item["id"]: item for item in CHAPTER3_REVIEWED_QUESTIONS}
    for qid in CHAPTER3_RANKING_READY_IDS:
        item = reviewed[qid]
        assert ranking_eligible(item) is True
        assert item["competitive"] is True
        assert item["confidence"] == "high"
        assert item["position"] == "neutral"
        assert item["claim_type"] == "text"
        assert item["sources"]


def test_hold_ids_are_fail_closed_with_explicit_reasons():
    for qid, reasons in CHAPTER3_RANKING_HOLD_REASONS.items():
        assert qid in CHAPTER3_RANKING_CANDIDATE_IDS
        assert reasons
        assert all(isinstance(reason, str) and reason.strip() for reason in reasons)
        assert not any("unknown_inspection_status" in reason for reason in reasons), (qid, reasons)


def test_source_status_vocabularies_do_not_overlap():
    assert CLAIM_READY_SOURCE_STATUSES
    assert LIMITED_SOURCE_STATUSES
    assert CLAIM_READY_SOURCE_STATUSES.isdisjoint(LIMITED_SOURCE_STATUSES)


def test_historical_audit_ready_set_is_exactly_the_later_explicit_authority():
    assert CHAPTER3_RANKING_READY_IDS == CHAPTER3_RANKING_AUTHORIZED_IDS
    assert not CHAPTER3_RANKING_HOLD_REASONS
    assert MANIFEST["ranking_pool_mutated"] is False
    assert MANIFEST["automatic_admission"] is False


def test_current_gameplay_consumes_authority_and_reviewed_challenge_taxonomy_only():
    authorized = set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    all_candidates = set(CHAPTER3_RANKING_CANDIDATE_IDS)
    unauthorized = all_candidates - authorized

    assert authorized <= _ids(questions.COMPETITIVE_POOL)
    assert authorized <= _ids(questions.BATTLE_POOL)
    assert unauthorized.isdisjoint(_ids(questions.COMPETITIVE_POOL))
    assert unauthorized.isdisjoint(_ids(questions.BATTLE_POOL))

    challenge_authorized = set()
    for level, pool in questions.CHALLENGE_POOLS.items():
        current = all_candidates & _ids(pool)
        expected = set(CHAPTER3_CHALLENGE_TAXONOMY[level])
        assert current == expected, level
        challenge_authorized |= current
    assert challenge_authorized == authorized


def test_normal_learning_bank_is_unchanged_by_ranking_lifecycle():
    chapter3_ids = _ids(questions.get_pool_by_key("chapter3"))
    reviewed_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    assert chapter3_ids == reviewed_ids
    assert len(chapter3_ids) == 165
    assert MANIFEST["normal_learning_mutated"] is False


def test_manifest_is_fail_closed_and_forbids_cross_lane_upgrade():
    assert MANIFEST["status"] == "RANKING_AUDITED_NOT_ADMITTED"
    assert MANIFEST["parent_product_head"] == "b84ea8559fddc3bade5044b9124ddab8784a0cc5"
    assert MANIFEST["fail_closed"] is True
    assert MANIFEST["limited_sources_are_holds"] is True
    assert MANIFEST["unknown_source_status_is_hold"] is True
    assert MANIFEST["root_only_nonprimary_source_is_hold"] is True
    assert MANIFEST["cross_lane_metadata_upgrade"] is False
    assert MANIFEST["root_primary_allowlist"] == ["sblgnt"]
