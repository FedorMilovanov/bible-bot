"""Effective Research Wave 3 authority resolver for Chapter 5."""

from .bank import CHAPTER5_STAGING_QUESTIONS

RESEARCH_SHA = "0142430af8ba80f28e0fd9cde669d32611a1d2af"
RESOLUTION_ORDER = (
    "base_candidate", "later_candidate_overrides", "source_upgrades_or_quorum",
    "wave3n_override", "mcq_prototype", "editorial_override",
)
HISTORICAL_HOLD_IDS = frozenset({"w3q_050", "w3q_051", "w3q_075"})
WAVE3N_CLOSED_IDS = HISTORICAL_HOLD_IDS
READY_IDS = frozenset({
    "w3q_046","w3q_048","w3q_049","w3q_052","w3q_055","w3q_056","w3q_057",
    "w3q_058","w3q_060","w3q_061","w3q_064","w3q_065","w3q_066","w3q_067",
    "w3q_068","w3q_069","w3q_070","w3q_072","w3q_076","w3q_081","w3q_082",
    "w3q_086","w3q_090","w3q_094","w3q_095","w3q_096",
})
HIGH_TC_RISK_IDS = frozenset({
    "w3q_050","w3q_051","w3q_068","w3q_075","w3q_125","w3q_126","w3q_127",
    "w3q_141","w3q_142","w3q_143","w3q_144",
})
MEDIUM_TC_RISK_IDS = frozenset({"w3q_084","w3q_117","w3q_128","w3q_135"})


def _effective_record(card: dict) -> dict:
    candidate_id = card["research_candidate_id"]
    risk = "high" if candidate_id in HIGH_TC_RISK_IDS else "medium" if candidate_id in MEDIUM_TC_RISK_IDS else "low"
    return {
        "candidate_id": candidate_id,
        "verse": card["verse"],
        "domain": card["topic"],
        "claim_type": card["claim_type"],
        "position": card["position"],
        "confidence": card["confidence"],
        "sources": list(card["sources"]),
        "owner_lane": "chapter5-production",
        "inspection_depth": "claim_sources_checked",
        "textual_critical_risk": risk,
        "disputed": card["confidence"] == "contested" or card["position"] == "project" or risk != "low",
        "historical_status": "HOLD" if candidate_id in HISTORICAL_HOLD_IDS else None,
        "effective_status": "READY" if candidate_id in READY_IDS else "READY_NONCOMPETITIVE",
        "authoring_disposition": "ADMIT_NORMAL_LEARNING",
        "ranking_disposition": "HOLD_NO_COMPETITIVE_AUTHORITY",
    }


EFFECTIVE_RESEARCH = [_effective_record(card) for card in CHAPTER5_STAGING_QUESTIONS]

TEXTUAL_CONTROL = {
    "5:2A": {"candidate_id": "w3q_050", "unit": "ἐπισκοποῦντες", "route": "ECM-based Williams-Horrell treatment; not direct dECM witness-table readback"},
    "5:2B": {"candidate_id": "w3q_051", "unit": "κατὰ θεόν", "route": "separate unit; Williams-Horrell plus published Stanojevic ECM comparison; not direct dECM readback"},
    "5:10": {"candidate_id": "w3q_068", "unit": "καταρτίσει / στηρίξει / σθενώσει / θεμελιώσει", "route": "SBLGNT/MorphGNT form-set observation only; no manuscript-unanimity claim"},
    "5:12": {"candidate_id": "w3q_075", "unit": "στῆτε / ἑστήκατε", "route": "explicit ECM-based published treatment; not direct dECM witness-table readback"},
}


def resolve_effective(candidate_id: str) -> dict:
    for record in EFFECTIVE_RESEARCH:
        if record["candidate_id"] == candidate_id:
            return dict(record)
    raise KeyError(candidate_id)


__all__ = ["EFFECTIVE_RESEARCH", "HISTORICAL_HOLD_IDS", "RESEARCH_SHA", "RESOLUTION_ORDER", "TEXTUAL_CONTROL", "WAVE3N_CLOSED_IDS", "resolve_effective"]
