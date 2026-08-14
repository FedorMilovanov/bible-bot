"""Explicit product disposition for all 72 effective Chapter 4 Research claims."""

from __future__ import annotations

from types import MappingProxyType

from .research_handoff import RESEARCH_HANDOFF_V2
from .review_registry import PRODUCT_REVIEW_BY_CARD_ID

_NON_PRODUCT_REASONS = {'w3q_007': 'RETAINED_RESEARCH_SUPPORT: historical Gentile-background nuance is retained behind the authored w3q_113 audience-background card; no duplicate product card.', 'w3q_036': 'RETAINED_RESEARCH_SUPPORT: edition-level 4:16 textual fact is supporting authority for authored w3q_121/w3q_122; textual criticism remains noncompetitive.', 'w3q_043': 'RETAINED_RESEARCH_SUPPORT: chapter-wide theological synthesis is useful explanatory context but is broader than a single unambiguous MCQ claim.', 'w3q_044': 'RETAINED_RESEARCH_SUPPORT: cross-chapter 4:7–11 / 5:1–4 leadership synthesis is retained as contextual support rather than duplicated in the Chapter 4 bank.', 'w3q_045': 'RETAINED_RESEARCH_SUPPORT: broader ἐν ᾧ semantic-range claim supports the narrower authored w3q_009 passage-level syntax card; not duplicated.', 'w3q_092': 'RETAINED_RESEARCH_SUPPORT: direct 4:14 Spirit/glory text observation is retained as support around authored w3q_031/w3q_093 and not duplicated.', 'w3q_098': 'RETAINED_RESEARCH_SUPPORT: project application for 4:3–4 is retained as supporting pastoral material; application coverage is intentionally selective.', 'w3q_099': 'RETAINED_RESEARCH_SUPPORT: project application of 4:7 is retained behind the direct-text w3q_016 and systematics-boundary w3q_017 cards.', 'w3q_101': 'RETAINED_RESEARCH_SUPPORT: project application of hospitality is retained behind the text/history coverage w3q_021/w3q_022.', 'w3q_104': 'RETAINED_RESEARCH_SUPPORT: project application of 4:19 overlaps the authored w3q_042 trust-and-do-good card and is not duplicated.', 'w3q_114': 'RETAINED_RESEARCH_SUPPORT: socio-cultural conversion reconstruction is retained as historical support; authored w3q_113/w3q_116 cover the safer audience/persecution distinctions.', 'w3q_115': 'RETAINED_RESEARCH_SUPPORT: social-boundary history for ἀλλοτριεπίσκοπος supports authored w3q_033 but remains too reconstruction-heavy for a second standalone card.', 'w3q_123': 'RETAINED_RESEARCH_SUPPORT: objective ECM/NA28 edition-attribution fact is a ranking discrepancy only; separate product ranking review concludes NO_RANKING_ADMISSION.', 'w3q_124': 'RETAINED_RESEARCH_SUPPORT: project application about naming editions is preserved as editorial guidance; direct product cards w3q_121/w3q_122 already teach the distinction.', 'w3q_129': 'RETAINED_RESEARCH_SUPPORT: οἰκονόμος lexical background supports authored w3q_024 and is not duplicated as a second lexical card.', 'w3q_131': 'RETAINED_RESEARCH_SUPPORT: πύρωσις lexical range supports authored w3q_029 and the Malachi boundary card w3q_038; no duplicate lexical card.', 'w3q_132': 'RETAINED_RESEARCH_SUPPORT: κτίστης lexical range supports authored w3q_041; wider civic-founder data remains background rather than a standalone MCQ.', 'w3q_138': 'RETAINED_RESEARCH_SUPPORT: manuscript-fact versus Ausgangstext methodology supports authored w3q_031/w3q_137 and remains textual-critical/noncompetitive.', 'w3q_139': 'RETAINED_RESEARCH_SUPPORT: Sinaiticus 4:16 reading is retained as named-witness support; it is not promoted into an original-text or ranking claim.', 'w3q_140': 'RETAINED_RESEARCH_SUPPORT: critical-text versus named-witness methodological distinction supports authored 4:16 TC cards and remains noncompetitive.'}

_claim_to_review = {
    review["research_claim_id"]: review
    for review in PRODUCT_REVIEW_BY_CARD_ID.values()
}

_records = {}
for research_claim_id, research in RESEARCH_HANDOFF_V2.items():
    review = _claim_to_review.get(research_claim_id)
    if review is not None:
        record = {
            "research_claim_id": research_claim_id,
            "research_effective_claim_digest": research["research_effective_claim_digest"],
            "product_disposition": "PRODUCT_CARD",
            "product_card_id": review["product_card_id"],
            "product_review_record_id": review["product_review_record_id"],
            "reason": "Independent v2 product review approved this bounded claim for normal learning only.",
        }
    else:
        reason = _NON_PRODUCT_REASONS.get(research_claim_id)
        if reason is None:
            raise ValueError(f"missing explicit Chapter 4 product disposition for {research_claim_id}")
        record = {
            "research_claim_id": research_claim_id,
            "research_effective_claim_digest": research["research_effective_claim_digest"],
            "product_disposition": "RETAINED_RESEARCH_SUPPORT",
            "reason": reason,
        }
        if research_claim_id == "w3q_123":
            record["ranking_review_id"] = "ch4rankv2_w3q_123_no_admission"
    _records[research_claim_id] = MappingProxyType(record)

CHAPTER4_PRODUCT_DISPOSITIONS = MappingProxyType(_records)

if len(CHAPTER4_PRODUCT_DISPOSITIONS) != 72:
    raise ValueError("Chapter 4 product disposition registry must cover all 72 Research claims")
if sum(
    record["product_disposition"] == "PRODUCT_CARD"
    for record in CHAPTER4_PRODUCT_DISPOSITIONS.values()
) != 52:
    raise ValueError("Chapter 4 product disposition registry must map exactly 52 claims to cards")
if sum(
    record["product_disposition"] == "RETAINED_RESEARCH_SUPPORT"
    for record in CHAPTER4_PRODUCT_DISPOSITIONS.values()
) != 20:
    raise ValueError("Chapter 4 product disposition registry must retain exactly 20 support claims")

__all__ = ["CHAPTER4_PRODUCT_DISPOSITIONS"]
