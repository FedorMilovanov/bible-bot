"""Machine-readable effective Research -> Chapter 4 production handoff.

Resolver authority follows the audited Research snapshot exactly:
BASE_CANDIDATE -> LATER_CANDIDATE_OVERRIDE -> SOURCE_UPGRADE/QUORUM ->
MCQ_PROTOTYPE -> MCQ_EDITORIAL_OVERRIDE.
Historical HOLD records remain in Research; effective later authority wins.
"""

from __future__ import annotations

from .authoring import CHAPTER4_STAGING_QUESTIONS

RESEARCH_AUTHORITY_SHA = "0142430af8ba80f28e0fd9cde669d32611a1d2af"
RESEARCH_PR = 183
RESOLVER_ORDER = (
    "BASE_CANDIDATE",
    "LATER_CANDIDATE_OVERRIDE",
    "SOURCE_UPGRADE_OR_QUORUM",
    "MCQ_PROTOTYPE",
    "MCQ_EDITORIAL_OVERRIDE",
)

# Effective Chapter-4 record surface after Wave 3n. The tuples preserve the
# research identity/classification even when editorial authoring intentionally
# declines to turn an overlapping claim into another production card.
_SPECS = [
    ("w3q_001","4:1","text","text","neutral","high"),("w3q_002","4:1","morphology","greek","neutral","high"),("w3q_003","4:1","morphology","greek","neutral","high"),("w3q_004","4:1","disputed","interpretation","neutral","medium"),("w3q_005","4:1-2","text","text","neutral","high"),("w3q_006","4:3","text","text","neutral","high"),("w3q_007","4:3","history","history","neutral","medium"),("w3q_008","4:4","text","text","neutral","high"),("w3q_009","4:4","syntax","interpretation","neutral","medium"),("w3q_010","4:5","text","text","neutral","high"),
    ("w3q_011","4:6","morphology","greek","neutral","high"),("w3q_012","4:6","disputed","interpretation","neutral","contested"),("w3q_013","4:6","course_position","interpretation","project","contested"),("w3q_014","4:6 / 3:19","disputed","interpretation","neutral","contested"),("w3q_015","4:7","morphology","greek","neutral","high"),("w3q_016","4:7","text","text","neutral","high"),("w3q_017","4:7","course_position","interpretation","project","medium"),("w3q_018","4:8","text","text","neutral","high"),("w3q_019","4:8 / Prov 10:12","intertext","interpretation","neutral","medium"),("w3q_020","4:8","application","interpretation","neutral","medium"),
    ("w3q_021","4:9","text","text","neutral","high"),("w3q_022","4:9","history","history","neutral","medium"),("w3q_023","4:10","text","text","neutral","high"),("w3q_024","4:10","lexical","greek","neutral","high"),("w3q_025","4:10-11","disputed","interpretation","neutral","medium"),("w3q_026","4:10-11","theology","interpretation","neutral","medium"),("w3q_027","4:10-11","course_position","interpretation","project","medium"),("w3q_028","4:12","text","text","neutral","high"),("w3q_029","4:12","lexical","greek","neutral","high"),("w3q_030","4:13","theology","text","neutral","high"),
    ("w3q_031","4:14","textual_criticism","text","neutral","high"),("w3q_032","4:15","text","text","neutral","high"),("w3q_033","4:15","lexical","interpretation","neutral","contested"),("w3q_034","4:16","text","text","neutral","high"),("w3q_035","4:16","history","history","neutral","medium"),("w3q_036","4:16","textual_criticism","text","neutral","high"),("w3q_037","4:17","theology","text","neutral","high"),("w3q_038","4:12-19 / Mal 3","intertext","interpretation","neutral","contested"),("w3q_039","4:18 / Prov 11:31","intertext","text","neutral","high"),("w3q_040","4:18 / Prov 11:31","intertext","interpretation","neutral","high"),
    ("w3q_041","4:19","theology","text","neutral","high"),("w3q_042","4:19","application","application","project","medium"),("w3q_043","4:1-19","theology","interpretation","neutral","medium"),("w3q_044","4:7-11 / 5:1-4","theology","interpretation","neutral","medium"),("w3q_045","4:4","syntax","interpretation","neutral","medium"),
    ("w3q_091","4:11","syntax","text","neutral","high"),("w3q_092","4:14","text","text","neutral","high"),("w3q_093","4:14","morphology","greek","neutral","high"),
    ("w3q_097","4:1-2","application","application","project","medium"),("w3q_098","4:3-4","application","application","project","medium"),("w3q_099","4:7","application","application","project","medium"),("w3q_100","4:8","application","application","project","medium"),("w3q_101","4:9","application","application","project","medium"),("w3q_102","4:10-11","application","application","project","medium"),("w3q_103","4:12-16","application","application","project","medium"),("w3q_104","4:19","application","application","project","medium"),
    ("w3q_113","4:3","history","history","neutral","medium"),("w3q_114","4:3-4","history","history","neutral","medium"),("w3q_115","4:15","history","history","neutral","medium"),("w3q_116","4:12-16","history","history","neutral","contested"),
    ("w3q_121","4:16","textual_criticism","text","neutral","high"),("w3q_122","4:16","textual_criticism","interpretation","neutral","medium"),("w3q_123","4:16","textual_criticism","text","neutral","high"),("w3q_124","4:16","textual_criticism","application","project","medium"),
    ("w3q_129","4:10","lexical","greek","neutral","medium"),("w3q_130","4:11","lexical","greek","neutral","medium"),("w3q_131","4:12","lexical","greek","neutral","medium"),("w3q_132","4:19","lexical","greek","neutral","medium"),
    ("w3q_137","4:14","textual_criticism","text","neutral","high"),("w3q_138","4:14","textual_criticism","interpretation","neutral","medium"),("w3q_139","4:16","textual_criticism","text","neutral","high"),("w3q_140","4:16","textual_criticism","interpretation","neutral","medium"),
]

_AUTHORED_BY_RESEARCH_ID = {item["research_id"]: item for item in CHAPTER4_STAGING_QUESTIONS}


def _inspection_depth(claim_type: str, authored: dict | None) -> str:
    if authored is not None:
        return str(authored["inspection_depth"])
    if claim_type == "text":
        return "research_authority_retained_not_duplicated_into_product_card"
    return "research_lane_depth_retained_at_authority_sha_not_promoted_globally"


def _record(spec: tuple[str, str, str, str, str, str]) -> dict:
    rid, verse, domain, claim_type, position, confidence = spec
    authored = _AUTHORED_BY_RESEARCH_ID.get(rid)
    is_objective = position == "neutral" and confidence == "high" and claim_type in {"text", "greek"}
    if authored is not None:
        disposition = "READY_FOR_AUTHORING"
        rationale = "Independent teachable claim admitted as a reviewed learning card; ranking remains separately closed."
        source_ids = list(authored["sources"])
        source_ids_complete = True
    else:
        disposition = "READY_NONCOMPETITIVE"
        rationale = "Effective research claim retained for audit but not duplicated into the 52-card bank because coverage overlaps a stronger authored card or remains better taught as supporting context."
        source_ids = ["sblgnt"] if claim_type in {"text", "greek", "interpretation", "application", "history"} else []
        source_ids_complete = False
    return {
        "id": rid,
        "verse": verse,
        "domain": domain,
        "claim_type": claim_type,
        "position": position,
        "confidence": confidence,
        "source_ids": source_ids,
        "source_ids_complete_in_bot_handoff": source_ids_complete,
        "owning_evidence_lane": str(authored["evidence_lane"]) if authored else domain,
        "actual_inspection_depth": _inspection_depth(claim_type, authored),
        "teachable": True,
        "objective_mcq_capable": is_objective,
        "honest_distractors_capable": authored is not None,
        "visible_course_position_required": position == "project",
        "production_disposition": disposition,
        "rationale": rationale,
        "research_authority_sha": RESEARCH_AUTHORITY_SHA,
    }


CHAPTER4_EFFECTIVE_RESEARCH_RECORDS = [_record(spec) for spec in _SPECS]

if len(CHAPTER4_EFFECTIVE_RESEARCH_RECORDS) != 72:
    raise ValueError("Chapter 4 effective Research resolver must expose exactly 72 records")
if len({record["id"] for record in CHAPTER4_EFFECTIVE_RESEARCH_RECORDS}) != 72:
    raise ValueError("duplicate Chapter 4 effective Research ids")
if any(record["production_disposition"] in {"QUARANTINE", "REJECT"} for record in CHAPTER4_EFFECTIVE_RESEARCH_RECORDS):
    raise ValueError("unexpected effective Research quarantine/reject in zero-HOLD snapshot")

CHAPTER4_RESEARCH_HANDOFF = {
    "schema_version": 1,
    "research_pr": RESEARCH_PR,
    "research_authority_sha": RESEARCH_AUTHORITY_SHA,
    "resolver_order": list(RESOLVER_ORDER),
    "effective_count": 72,
    "research_prototype_count": 32,
    "research_hold_count": 0,
    "research_competitive_candidate_count": 0,
    "authored_count": len(CHAPTER4_STAGING_QUESTIONS),
    "records": CHAPTER4_EFFECTIVE_RESEARCH_RECORDS,
    "invariants": [
        "SOURCE_FOUND_NE_CLAIM_PROVED",
        "MORPHOLOGY_NE_EXEGESIS",
        "PROJECT_POSITION_NE_NEUTRAL_FACT",
        "ECM_TEXT_DECISION_NE_MANUSCRIPT_UNANIMITY",
        "ZERO_RESEARCH_HOLDS_NE_PRODUCTION_READY",
        "RESEARCH_PR_NE_BOT_PRODUCTION",
    ],
}

__all__ = [
    "CHAPTER4_EFFECTIVE_RESEARCH_RECORDS",
    "CHAPTER4_RESEARCH_HANDOFF",
    "RESEARCH_AUTHORITY_SHA",
    "RESOLVER_ORDER",
]
