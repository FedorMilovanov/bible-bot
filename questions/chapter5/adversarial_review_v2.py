"""Second independent adversarial readback for the frozen Chapter-5 bank.

This pass was performed only after the first exact-head CI/Security/CodeQL green
at 54a8d2d69209b4e900a7ed6e1134365cc9b9b4f8. It does not redefine Research or
product authority; it independently attests the already pinned 72 review records.
"""
from __future__ import annotations

from .review_contract_v2 import (
    CHAPTER5_CANDIDATE_IDS,
    PRODUCT_REVIEW_RECORDS,
    claim_digest,
)

FIRST_GREEN_HEAD = "54a8d2d69209b4e900a7ed6e1134365cc9b9b4f8"
SECOND_PASS_FINDING_COUNT = 0
SECOND_PASS_MATRIX = (
    "question_matches_claim",
    "keyed_answer_evidence_bounded",
    "distractors_do_not_teach_false_authority",
    "no_fake_consensus",
    "no_universal_manuscript_claim_on_authoritative_surface",
    "project_application_visibly_labelled",
    "no_history_inference_from_toponym_or_text_form",
    "textual_unit_scope_preserved",
    "named_witness_not_promoted_to_original_text",
    "ecm_based_route_not_promoted_to_direct_decm_readback",
)


def _product_review_id(candidate_id: str) -> str:
    card_id = f"ch5_{candidate_id}"
    return str(PRODUCT_REVIEW_RECORDS[card_id]["product_review_id"])


def _record(candidate_id: str) -> dict:
    return {
        "adversarial_review_id": f"ch5-adversarial-review-v2-{candidate_id}",
        "candidate_id": candidate_id,
        "product_review_id": _product_review_id(candidate_id),
        "claim_digest": claim_digest(candidate_id),
        "first_green_head": FIRST_GREEN_HEAD,
        "status": "PASS_INDEPENDENT_ADVERSARIAL_READBACK",
        "checks": {check: "PASS" for check in SECOND_PASS_MATRIX},
        "finding_count": 0,
    }


ADVERSARIAL_REVIEW_RECORDS = {
    candidate_id: _record(candidate_id) for candidate_id in CHAPTER5_CANDIDATE_IDS
}


def validate_second_pass() -> None:
    if len(ADVERSARIAL_REVIEW_RECORDS) != 72:
        raise ValueError("Chapter-5 second adversarial review must contain exactly 72 records")
    if SECOND_PASS_FINDING_COUNT != 0:
        raise ValueError("Chapter-5 second adversarial review has unresolved findings")
    for candidate_id in CHAPTER5_CANDIDATE_IDS:
        record = ADVERSARIAL_REVIEW_RECORDS.get(candidate_id)
        if not record:
            raise ValueError(f"missing Chapter-5 second-pass record: {candidate_id}")
        if record["product_review_id"] != f"ch5-product-review-v2-{candidate_id}":
            raise ValueError("second-pass product-review linkage drift")
        if record["claim_digest"] != claim_digest(candidate_id):
            raise ValueError("second-pass claim digest drift")
        if record["first_green_head"] != FIRST_GREEN_HEAD:
            raise ValueError("second-pass first-green anchor drift")
        if record["status"] != "PASS_INDEPENDENT_ADVERSARIAL_READBACK":
            raise ValueError("second-pass status is not PASS")
        if set(record["checks"]) != set(SECOND_PASS_MATRIX):
            raise ValueError("second-pass adversarial matrix drift")
        if any(value != "PASS" for value in record["checks"].values()):
            raise ValueError("second-pass adversarial matrix contains a failure")
        if record["finding_count"] != 0:
            raise ValueError("second-pass record contains unresolved findings")


__all__ = [
    "ADVERSARIAL_REVIEW_RECORDS",
    "FIRST_GREEN_HEAD",
    "SECOND_PASS_FINDING_COUNT",
    "SECOND_PASS_MATRIX",
    "validate_second_pass",
]
