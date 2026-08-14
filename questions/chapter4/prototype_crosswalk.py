"""Agent-E v2 prototype reconciliation for all 32 Chapter 4 prototypes."""

from __future__ import annotations

from collections import Counter
from types import MappingProxyType

from .review_registry import PRODUCT_REVIEW_BY_CARD_ID

_ROWS = [
    ("w3mcq_001","w3q_001","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_002","w3q_003","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_003","w3q_005","REJECT_AS_PRODUCT_TEMPLATE",("REFERENCE_DRIFT",),(),"UNAVAILABLE"),
    ("w3mcq_004","w3q_008","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_005","w3q_011","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_006","w3q_016","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_007","w3q_018","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_008","w3q_019","NONCOMPETITIVE_ONLY",("NONCOMPETITIVE_AUTHORITY_SHAPE",),(),"PRESENT"),
    ("w3mcq_009","w3q_021","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_010","w3q_023","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_011","w3q_029","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_012","w3q_030","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_013","w3q_032","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_014","w3q_034","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_015","w3q_039","NONCOMPETITIVE_ONLY",("LOW_AUTOMATED_CLAIM_ALIGNMENT_SIGNAL","NONCOMPETITIVE_AUTHORITY_SHAPE"),(),"LOW_REQUIRES_HUMAN_SEMANTIC_CHECK"),
    ("w3mcq_016","w3q_041","SAFE_TEMPLATE",("STRUCTURALLY_BOUNDED_NEUTRAL_TEMPLATE_ONLY",),(),"PRESENT"),
    ("w3mcq_033","w3q_004","NEEDS_REWRITE",("DISTRACTOR_FALSE_CERTAINTY_OR_EVIDENCE_LAUNDERING",),("ABSOLUTE_ALWAYS_NEVER","LEXICAL_EXCLUSIVITY"),"PRESENT"),
    ("w3mcq_034","w3q_009","NONCOMPETITIVE_ONLY",("NONCOMPETITIVE_AUTHORITY_SHAPE",),(),"PRESENT"),
    ("w3mcq_035","w3q_012","NONCOMPETITIVE_ONLY",("NONCOMPETITIVE_AUTHORITY_SHAPE",),(),"PRESENT"),
    ("w3mcq_036","w3q_013","NEEDS_REWRITE",("DISTRACTOR_FALSE_CERTAINTY_OR_EVIDENCE_LAUNDERING",),("CONSENSUS_TOTALIZATION","PROJECT_AS_NEUTRAL_CONSENSUS","GRAMMAR_REQUIRES_EXEGESIS"),"PRESENT"),
    ("w3mcq_037","w3q_014","REJECT_AS_PRODUCT_TEMPLATE",("REFERENCE_DRIFT",),(),"UNAVAILABLE"),
    ("w3mcq_038","w3q_017","NEEDS_REWRITE",("DISTRACTOR_FALSE_CERTAINTY_OR_EVIDENCE_LAUNDERING",),("ABSOLUTE_ALWAYS_NEVER",),"PRESENT"),
    ("w3mcq_039","w3q_020","NONCOMPETITIVE_ONLY",("NONCOMPETITIVE_AUTHORITY_SHAPE",),(),"PRESENT"),
    ("w3mcq_040","w3q_022","NEEDS_REWRITE",("DISTRACTOR_FALSE_CERTAINTY_OR_EVIDENCE_LAUNDERING",),("GENERIC_PROOF_CLAIM","HISTORICAL_LEGAL_CERTAINTY"),"PRESENT"),
    ("w3mcq_041","w3q_025","NEEDS_REWRITE",("DISTRACTOR_FALSE_CERTAINTY_OR_EVIDENCE_LAUNDERING",),("GENERIC_PROOF_CLAIM",),"PRESENT"),
    ("w3mcq_042","w3q_027","NEEDS_REWRITE",("DISTRACTOR_FALSE_CERTAINTY_OR_EVIDENCE_LAUNDERING",),("ABSOLUTE_ALWAYS_NEVER",),"PRESENT"),
    ("w3mcq_043","w3q_033","NEEDS_REWRITE",("DISTRACTOR_FALSE_CERTAINTY_OR_EVIDENCE_LAUNDERING",),("ABSOLUTE_CERTAINTY","LEXICAL_EXCLUSIVITY"),"PRESENT"),
    ("w3mcq_044","w3q_035","NEEDS_REWRITE",("DISTRACTOR_FALSE_CERTAINTY_OR_EVIDENCE_LAUNDERING",),("TEXTUAL_INSERTION_CERTAINTY","GENERIC_PROOF_CLAIM","HISTORICAL_LEGAL_CERTAINTY"),"PRESENT"),
    ("w3mcq_045","w3q_121","NEEDS_REWRITE",("DISTRACTOR_FALSE_CERTAINTY_OR_EVIDENCE_LAUNDERING",),("MANUSCRIPT_UNANIMITY","EDITION_IDENTITY_COLLAPSE","TEXTUAL_INSERTION_CERTAINTY"),"PRESENT"),
    ("w3mcq_046","w3q_122","NEEDS_REWRITE",("DISTRACTOR_FALSE_CERTAINTY_OR_EVIDENCE_LAUNDERING",),("TOTAL_EVIDENCE_DISMISSAL","ABSOLUTE_ALWAYS_NEVER","CBGM_AUTOMATION_MYTH"),"PRESENT"),
    ("w3mcq_047","w3q_038","REJECT_AS_PRODUCT_TEMPLATE",("REFERENCE_DRIFT",),(),"UNAVAILABLE"),
    ("w3mcq_048","w3q_040","NONCOMPETITIVE_ONLY",("NONCOMPETITIVE_AUTHORITY_SHAPE",),(),"PRESENT"),
]

_review_by_prototype = {}
for review in PRODUCT_REVIEW_BY_CARD_ID.values():
    prototype_id = review["prototype_review"]["research_prototype_id"]
    if prototype_id:
        _review_by_prototype[prototype_id] = review

_records = {}
for prototype_id, claim_id, classification, reasons, risks, alignment in _ROWS:
    review = _review_by_prototype.get(prototype_id)
    if review is None:
        raise ValueError(f"Chapter 4 prototype lacks product reconciliation: {prototype_id}")
    mechanical_copy_forbidden = classification != "SAFE_TEMPLATE"
    if classification == "REJECT_AS_PRODUCT_TEMPLATE":
        product_resolution = "INDEPENDENT_PRODUCT_REWRITE_AFTER_REJECTED_TEMPLATE"
    elif classification == "SAFE_TEMPLATE":
        product_resolution = "INDEPENDENT_PRODUCT_REVIEW_OF_SAFE_TEMPLATE"
    else:
        product_resolution = f"INDEPENDENT_PRODUCT_REWRITE_AFTER_{classification}"
    _records[prototype_id] = MappingProxyType({
        "research_prototype_id": prototype_id,
        "research_claim_id": claim_id,
        "agent_e_classification": classification,
        "agent_e_reasons": tuple(reasons),
        "agent_e_rewrite_risk_ids": tuple(risks),
        "claim_alignment_signal": alignment,
        "product_card_id": review["product_card_id"],
        "product_review_record_id": review["product_review_record_id"],
        "product_resolution": product_resolution,
        "mechanical_copy_forbidden": mechanical_copy_forbidden,
        "prototype_is_product_authority": False,
    })

CHAPTER4_PROTOTYPE_CROSSWALK = MappingProxyType(_records)

_expected = Counter({
    "SAFE_TEMPLATE": 13,
    "NEEDS_REWRITE": 10,
    "NONCOMPETITIVE_ONLY": 6,
    "REJECT_AS_PRODUCT_TEMPLATE": 3,
})
if len(CHAPTER4_PROTOTYPE_CROSSWALK) != 32:
    raise ValueError("Chapter 4 prototype crosswalk must cover all 32 prototypes")
if Counter(row["agent_e_classification"] for row in _records.values()) != _expected:
    raise ValueError("Chapter 4 Agent-E prototype classification counts drifted")
for prototype_id in ("w3mcq_003", "w3mcq_037", "w3mcq_047"):
    row = CHAPTER4_PROTOTYPE_CROSSWALK[prototype_id]
    if row["agent_e_classification"] != "REJECT_AS_PRODUCT_TEMPLATE":
        raise ValueError(f"{prototype_id} reference-drift rejection was silently rehabilitated")
    if not row["product_resolution"].startswith("INDEPENDENT_PRODUCT_REWRITE"):
        raise ValueError(f"{prototype_id} must remain an independent product rewrite")

__all__ = ["CHAPTER4_PROTOTYPE_CROSSWALK"]
