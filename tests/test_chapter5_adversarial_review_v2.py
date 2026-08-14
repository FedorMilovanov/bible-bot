import json
from pathlib import Path

from questions.chapter5.adversarial_review_v2 import (
    ADVERSARIAL_REVIEW_RECORDS,
    FIRST_GREEN_HEAD,
    SECOND_PASS_FINDING_COUNT,
    SECOND_PASS_MATRIX,
    validate_second_pass,
)
from questions.chapter5.review_contract_v2 import PROTOTYPE_AUDIT_RECORDS


def test_second_adversarial_readback_is_72_of_72_and_anchored_after_first_green():
    validate_second_pass()
    assert FIRST_GREEN_HEAD == "54a8d2d69209b4e900a7ed6e1134365cc9b9b4f8"
    assert len(ADVERSARIAL_REVIEW_RECORDS) == 72
    assert SECOND_PASS_FINDING_COUNT == 0
    assert len(SECOND_PASS_MATRIX) == 10
    assert all(record["finding_count"] == 0 for record in ADVERSARIAL_REVIEW_RECORDS.values())


def test_materialized_prototype_audit_matches_runtime_contract():
    path = Path(__file__).resolve().parents[1] / "data" / "chapter5-prototype-audit-v2.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = {record["prototypeId"]: record for record in payload["records"]}
    assert payload["prototypeCount"] == 32
    assert len(records) == 32
    assert set(records) == set(PROTOTYPE_AUDIT_RECORDS)
    assert payload["publicationAuthority"] is False
    assert payload["rankingAuthority"] is False
    for prototype_id, runtime in PROTOTYPE_AUDIT_RECORDS.items():
        materialized = records[prototype_id]
        assert materialized["candidateId"] == runtime["candidate_id"]
        assert materialized["productDisposition"] == runtime["product_disposition"]
        assert materialized["disposition"] == runtime["prototype_disposition"]
