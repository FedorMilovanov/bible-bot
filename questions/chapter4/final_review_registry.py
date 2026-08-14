"""Final immutable Chapter 4 review registry after the second content pass.

The first-green registry remains historically inspectable in review_registry.py.
Cards changed by the required post-green adversarial pass receive new immutable
review-record IDs and content digests; unchanged records retain their IDs.
"""

from __future__ import annotations

from types import MappingProxyType

from .review_registry import (
    PRODUCT_REVIEW_BY_CARD_ID as FIRST_GREEN_REVIEW_BY_CARD_ID,
    product_card_content_digest,
)
from .second_pass_revisions import REVIEW_RECORD_REVISIONS


def _finalize_record(record: dict) -> MappingProxyType:
    card_id = record["product_card_id"]
    if card_id not in REVIEW_RECORD_REVISIONS:
        return MappingProxyType(dict(record))
    review_record_id, content_digest = REVIEW_RECORD_REVISIONS[card_id]
    revised = dict(record)
    revised["product_review_record_id"] = review_record_id
    revised["product_card_content_digest_sha256"] = content_digest
    return MappingProxyType(revised)


_final_records = [
    _finalize_record(record)
    for record in FIRST_GREEN_REVIEW_BY_CARD_ID.values()
]

PRODUCT_REVIEW_REGISTRY = MappingProxyType({
    record["product_review_record_id"]: record
    for record in _final_records
})
PRODUCT_REVIEW_BY_CARD_ID = MappingProxyType({
    record["product_card_id"]: record
    for record in _final_records
})

if len(PRODUCT_REVIEW_REGISTRY) != 52 or len(PRODUCT_REVIEW_BY_CARD_ID) != 52:
    raise ValueError("final Chapter 4 v2 registry must contain exactly 52 review records")
if set(REVIEW_RECORD_REVISIONS) - set(PRODUCT_REVIEW_BY_CARD_ID):
    raise ValueError("second-pass review revision references missing Chapter 4 card")

__all__ = [
    "PRODUCT_REVIEW_REGISTRY",
    "PRODUCT_REVIEW_BY_CARD_ID",
    "product_card_content_digest",
]
