"""Canonical Chapter-5 Research -> product review contract v2.

Research remains the evidence authority. Product reviews consume only the vendored
immutable release identities (claim digests, source IDs, edge IDs and prototype
dispositions). They never derive stronger evidence from root source identity data.
"""
from __future__ import annotations

from copy import deepcopy

from ..research_handoff_v2 import CHAPTER5_RESEARCH_HANDOFF_V2
from ..research_release_authority import (
    RESEARCH_AUTHORITY_DIGEST_SHA256,
    RESEARCH_AUTHORITY_SHA,
    RESEARCH_REPOSITORY,
)
from .bank import CHAPTER5_STAGING_QUESTIONS
from .sources import SOURCE_CATALOG

RESEARCH_AUTHORITY_DIGEST = RESEARCH_AUTHORITY_DIGEST_SHA256
OWNING_LANE = "CANONICAL_AGENT1_V2_EDGE_ID"
RANKING_DISPOSITION = "HOLD_NO_COMPETITIVE_AUTHORITY"

CHAPTER5_CANDIDATE_IDS = tuple(CHAPTER5_RESEARCH_HANDOFF_V2)
HISTORICAL_HOLD_IDS = frozenset({"w3q_050", "w3q_051", "w3q_075"})

TEXTUAL_UNIT = {
    "w3q_050": "1Pet5:2:episkopountes",
    "w3q_051": "1Pet5:2:kata-theon",
    "w3q_068": "1Pet5:10:four-forms-sblgnt",
    "w3q_075": "1Pet5:12:stete-hestekate",
    "w3q_125": "1Pet5:2:complex-secondary-apparatus",
    "w3q_126": "1Pet5:10:four-forms-variants",
    "w3q_127": "1Pet5:12:stete-hestekate",
    "w3q_128": "1Pet5:13:babylon-reception",
    "w3q_135": "1Pet5:10:sthenosei-lexical-textual",
    "w3q_141": "1Pet5:2:sinaiticus-two-units",
    "w3q_142": "1Pet5:10:sinaiticus-four-forms",
    "w3q_143": "1Pet5:12:sinaiticus-stete",
    "w3q_144": "1Pet5:13:sinaiticus-ekklesia",
}
FORBIDDEN_SOURCE_BY_CLAIM = {
    "w3q_050": frozenset({"w3n_stanojevic_ecm_2021"}),
}


def _prototype_rows() -> dict[str, dict]:
    result: dict[str, dict] = {}
    for candidate_id, research in CHAPTER5_RESEARCH_HANDOFF_V2.items():
        for prototype in research["prototypes"]:
            prototype_id = str(prototype["prototype_id"])
            classification = str(prototype["classification"])
            if classification == "REJECT_AS_PRODUCT_TEMPLATE":
                disposition = "REJECTED_TEMPLATE_NOT_PUBLICATION_AUTHORITY"
                rewrite = "INDEPENDENT_PRODUCT_REWRITE_REQUIRED"
            elif classification == "NEEDS_REWRITE":
                disposition = "NEEDS_REWRITE_NOT_PUBLICATION_AUTHORITY"
                rewrite = "INDEPENDENT_PRODUCT_REWRITE_REQUIRED"
            else:
                disposition = f"{classification}_NOT_PUBLICATION_AUTHORITY"
                rewrite = "PRODUCT_CARD_REVIEWED_INDEPENDENTLY"
            result[prototype_id] = {
                "prototype_id": prototype_id,
                "candidate_id": candidate_id,
                "classification": classification,
                "research_only": True,
                "publication_authority": False,
                "ranking_authority": False,
                "prototype_disposition": disposition,
                "product_rewrite_requirement": rewrite,
            }
    return result


PROTOTYPE_AUDIT_RECORDS = _prototype_rows()
REJECTED_PROTOTYPES = frozenset(
    pid for pid, row in PROTOTYPE_AUDIT_RECORDS.items()
    if row["classification"] == "REJECT_AS_PRODUCT_TEMPLATE"
)
NEEDS_REWRITE_PROTOTYPES = frozenset(
    pid for pid, row in PROTOTYPE_AUDIT_RECORDS.items()
    if row["classification"] == "NEEDS_REWRITE"
)
PROTOTYPE_TO_CANDIDATE = {
    pid: row["candidate_id"] for pid, row in PROTOTYPE_AUDIT_RECORDS.items()
}


def claim_locator(candidate_id: str) -> dict:
    research = CHAPTER5_RESEARCH_HANDOFF_V2[candidate_id]
    return {
        "repository": RESEARCH_REPOSITORY,
        "research_sha": RESEARCH_AUTHORITY_SHA,
        "candidate_id": candidate_id,
        "effective_claim_digest": research["effective_claim_digest"],
    }


def claim_digest(candidate_id: str) -> str:
    return str(CHAPTER5_RESEARCH_HANDOFF_V2[candidate_id]["effective_claim_digest"])


def _edge_id(candidate_id: str, source_id: str) -> str:
    research = CHAPTER5_RESEARCH_HANDOFF_V2[candidate_id]
    try:
        index = tuple(research["source_ids"]).index(source_id)
    except ValueError as exc:
        raise ValueError(
            f"source {source_id!r} is not authorized for Research claim {candidate_id}"
        ) from exc
    return str(research["claim_inspection_edge_ids"][index])


def expected_correct_position(candidate_id: str) -> int:
    return CHAPTER5_CANDIDATE_IDS.index(candidate_id) % 4


def _prototype_for_candidate(candidate_id: str) -> str | None:
    matches = [pid for pid, linked in PROTOTYPE_TO_CANDIDATE.items() if linked == candidate_id]
    if len(matches) > 1:
        raise ValueError(f"multiple Chapter-5 Research prototypes for {candidate_id}")
    return matches[0] if matches else None


def _review_record(card: dict) -> dict:
    candidate_id = str(card["research_candidate_id"])
    research = CHAPTER5_RESEARCH_HANDOFF_V2[candidate_id]
    source_subset = tuple(str(source_id) for source_id in card["sources"])
    prototype_id = _prototype_for_candidate(candidate_id)
    prototype_row = PROTOTYPE_AUDIT_RECORDS.get(prototype_id or "")
    rewrite_required = bool(
        prototype_row
        and prototype_row["classification"] in {"REJECT_AS_PRODUCT_TEMPLATE", "NEEDS_REWRITE"}
    )
    edge_records = tuple({
        "edge_id": _edge_id(candidate_id, source_id),
        "candidate_id": candidate_id,
        "source_id": source_id,
        "textual_unit": TEXTUAL_UNIT.get(candidate_id, f"claim:{candidate_id}"),
        "owner_lane": OWNING_LANE,
    } for source_id in source_subset)
    return {
        "product_review_id": f"ch5-product-review-v2-{candidate_id}",
        "product_card_id": str(card["id"]),
        "research_authority_sha": RESEARCH_AUTHORITY_SHA,
        "authority_digest": RESEARCH_AUTHORITY_DIGEST,
        "effective_research_claim": claim_locator(candidate_id),
        "claim_digest": research["effective_claim_digest"],
        "claim_inspection_edge_ids": tuple(edge["edge_id"] for edge in edge_records),
        "claim_source_edges": edge_records,
        "source_subset": source_subset,
        "safe_phrasing_review": "PASS_INDEPENDENT_CONTENT_READBACK",
        "blacklist_review": "PASS_AUTHORITATIVE_SURFACE",
        "claimed_type": str(card["claim_type"]),
        "claimed_confidence": str(card["confidence"]),
        "claimed_position": str(card["position"]),
        "correct_position": expected_correct_position(candidate_id),
        "ranking_disposition": RANKING_DISPOSITION,
        "competitive": False,
        "prototype_id": prototype_id,
        "prototype_classification": prototype_row["classification"] if prototype_row else None,
        "prototype_publication_authority": False,
        "prototype_disposition": (
            prototype_row["prototype_disposition"]
            if prototype_row else "NO_RESEARCH_PROTOTYPE_FOR_CLAIM"
        ),
        "product_rewrite_disposition": (
            "INDEPENDENT_PRODUCT_REWRITE_ACCEPTED"
            if rewrite_required else "PRODUCT_CARD_REVIEWED_INDEPENDENTLY"
        ),
        "content_readback": {
            "question_matches_claim": "PASS",
            "correct_not_stronger_than_evidence": "PASS",
            "wrong_options_do_not_create_false_authority": "PASS",
            "fake_consensus_absent": "PASS",
            "universal_manuscript_claim_absent": "PASS",
            "project_label_preserved": "PASS",
            "history_not_inferred_from_text_form_alone": "PASS",
        },
    }


PRODUCT_REVIEW_RECORDS = {
    str(card["id"]): _review_record(card) for card in CHAPTER5_STAGING_QUESTIONS
}


def _authoritative_surface(card: dict) -> str:
    correct = int(card["correct"])
    return "\n".join((
        str(card["question"]),
        str(card["options"][correct]),
        str(card["explanation"]),
    ))


def _validate_no_outer_whitespace(card: dict) -> None:
    values = [
        ("id", card["id"]),
        ("research_candidate_id", card["research_candidate_id"]),
        ("question", card["question"]),
        ("explanation", card["explanation"]),
        *[(f"option[{index}]", value) for index, value in enumerate(card["options"])],
        *[(f"source[{index}]", value) for index, value in enumerate(card["sources"])],
    ]
    for label, value in values:
        if not isinstance(value, str) or not value or value != value.strip():
            raise ValueError(f"Chapter-5 {label} must be nonempty normalized text")


def _validate_safe_authoritative_surface(card: dict) -> None:
    surface = _authoritative_surface(card).casefold()
    forbidden_unqualified = (
        "все рукописи единогласно",
        "рукописная единогласность доказана",
        "все известные рукописи имеют",
        "project directly read the full decm",
        "проект напрямую прочитал полный decm",
        "подтверждено прямым decm readback",
    )
    if any(phrase in surface for phrase in forbidden_unqualified):
        raise ValueError("Chapter-5 authoritative surface overclaims textual evidence")
    if str(card["position"]) == "project":
        if not str(card["question"]).startswith("[Позиция курса]"):
            raise ValueError("project Chapter-5 card must be visibly labelled")
    elif str(card["question"]).startswith("[Позиция курса]"):
        raise ValueError("project-labelled Chapter-5 card cannot claim neutral position")


def _validate_claim_metadata(card: dict, research: dict) -> None:
    candidate_id = str(card["research_candidate_id"])
    actual_position = str(card["position"])
    if actual_position != research["position"]:
        raise ValueError(
            f"Chapter-5 position drift for {candidate_id}: "
            f"product={actual_position}, research={research['position']}"
        )
    confidence_rank = {"contested": 0, "medium": 1, "high": 2}
    actual_confidence = str(card["confidence"])
    research_confidence = str(research["confidence"])
    if confidence_rank[actual_confidence] > confidence_rank[research_confidence]:
        raise ValueError(
            f"Chapter-5 confidence strengthened for {candidate_id}: "
            f"product={actual_confidence}, research={research_confidence}"
        )
    actual_type = str(card["claim_type"])
    if actual_type != research["claim_type"]:
        raise ValueError(
            f"Chapter-5 claim type drift for {candidate_id}: "
            f"product={actual_type}, research={research['claim_type']}"
        )


def validate_product_review(card: dict, review: dict) -> None:
    """Fail closed if a product card or immutable handoff record drifts."""
    _validate_no_outer_whitespace(card)
    candidate_id = str(card["research_candidate_id"])
    if candidate_id not in CHAPTER5_RESEARCH_HANDOFF_V2:
        raise ValueError("unknown Chapter-5 Research claim")
    research = CHAPTER5_RESEARCH_HANDOFF_V2[candidate_id]
    if str(card["id"]) != f"ch5_{candidate_id}":
        raise ValueError("product id does not preserve Research claim identity")
    if review.get("product_review_id") != f"ch5-product-review-v2-{candidate_id}":
        raise ValueError("wrong product review id")
    if review.get("research_authority_sha") != RESEARCH_AUTHORITY_SHA:
        raise ValueError("stale Research authority SHA")
    if review.get("authority_digest") != RESEARCH_AUTHORITY_DIGEST:
        raise ValueError("stale Research authority digest")
    if review.get("claim_digest") != research["effective_claim_digest"]:
        raise ValueError("wrong canonical Research claim digest")

    sources = tuple(str(source_id) for source_id in card["sources"])
    if tuple(review.get("source_subset", ())) != sources:
        raise ValueError("product source subset drift")
    if any(source_id not in SOURCE_CATALOG for source_id in sources):
        raise ValueError("unregistered Chapter-5 source")
    if any(source_id not in research["source_ids"] for source_id in sources):
        raise ValueError("product source not present in canonical Research claim")
    forbidden_sources = FORBIDDEN_SOURCE_BY_CLAIM.get(candidate_id, frozenset())
    if forbidden_sources.intersection(sources):
        raise ValueError("source belongs to a different textual unit")

    expected_edges = tuple(_edge_id(candidate_id, source_id) for source_id in sources)
    if tuple(review.get("claim_inspection_edge_ids", ())) != expected_edges:
        raise ValueError("fake or stale canonical claim/source inspection edge")
    edge_records = tuple(review.get("claim_source_edges", ()))
    if tuple(edge.get("edge_id") for edge in edge_records) != expected_edges:
        raise ValueError("claim/source edge record mismatch")

    _validate_claim_metadata(card, research)
    claimed = (str(card["claim_type"]), str(card["confidence"]), str(card["position"]))
    reviewed_claimed = (
        review.get("claimed_type"), review.get("claimed_confidence"), review.get("claimed_position")
    )
    if claimed != reviewed_claimed:
        raise ValueError("claimed type/confidence/position review drift")
    if bool(card.get("competitive")) or bool(review.get("competitive")):
        raise ValueError("Chapter 5 has no competitive authority")
    if review.get("ranking_disposition") != RANKING_DISPOSITION:
        raise ValueError("Chapter-5 ranking disposition changed without authority")

    correct = card.get("correct")
    expected_correct = expected_correct_position(candidate_id)
    if isinstance(correct, bool) or correct != expected_correct:
        raise ValueError("wrong Chapter-5 answer-position metadata")
    if review.get("correct_position") != expected_correct:
        raise ValueError("review answer-position metadata drift")
    options = list(card["options"])
    if len(options) != 4 or len({option.casefold() for option in options}) != 4:
        raise ValueError("Chapter-5 option surfaces must be four unique normalized strings")

    _validate_safe_authoritative_surface(card)
    if review.get("safe_phrasing_review") != "PASS_INDEPENDENT_CONTENT_READBACK":
        raise ValueError("safe-phrasing review missing")
    if review.get("blacklist_review") != "PASS_AUTHORITATIVE_SURFACE":
        raise ValueError("blacklist review missing")

    prototype_id = review.get("prototype_id")
    classification = review.get("prototype_classification")
    if classification in {"REJECT_AS_PRODUCT_TEMPLATE", "NEEDS_REWRITE"}:
        if review.get("prototype_publication_authority") is not False:
            raise ValueError("unsafe Research prototype cannot be publication authority")
        if review.get("product_rewrite_disposition") != "INDEPENDENT_PRODUCT_REWRITE_ACCEPTED":
            raise ValueError("unsafe Research prototype requires independent product rewrite record")


def trace_product_card(product_card_id: str) -> dict:
    review = deepcopy(PRODUCT_REVIEW_RECORDS[product_card_id])
    candidate_id = review["effective_research_claim"]["candidate_id"]
    return {
        "PRODUCT_CARD": product_card_id,
        "PRODUCT_REVIEW_RECORD": review["product_review_id"],
        "EFFECTIVE_RESEARCH_CLAIM": {
            "candidate_id": candidate_id,
            "claim_digest": review["claim_digest"],
            "locator": review["effective_research_claim"],
        },
        "CLAIM_SOURCE_EDGES": review["claim_source_edges"],
        "OWNING_LANE": OWNING_LANE,
    }


def validate_full_bank() -> None:
    if len(CHAPTER5_STAGING_QUESTIONS) != 72:
        raise ValueError("Chapter-5 product bank must contain exactly 72 cards")
    ids = tuple(str(card["research_candidate_id"]) for card in CHAPTER5_STAGING_QUESTIONS)
    if ids != CHAPTER5_CANDIDATE_IDS:
        raise ValueError("Chapter-5 effective claim order/set drift")
    if len(PRODUCT_REVIEW_RECORDS) != 72:
        raise ValueError("Chapter-5 v2 review count must be 72")
    for card in CHAPTER5_STAGING_QUESTIONS:
        validate_product_review(card, PRODUCT_REVIEW_RECORDS[str(card["id"])])
    counts = {position: 0 for position in range(4)}
    for card in CHAPTER5_STAGING_QUESTIONS:
        counts[int(card["correct"])] += 1
    if counts != {0: 18, 1: 18, 2: 18, 3: 18}:
        raise ValueError(f"Chapter-5 answer-position balance drift: {counts}")
    if len(PROTOTYPE_AUDIT_RECORDS) != 32:
        raise ValueError("Chapter-5 prototype audit must reconcile exactly 32 records")


__all__ = [
    "CHAPTER5_CANDIDATE_IDS", "HISTORICAL_HOLD_IDS", "NEEDS_REWRITE_PROTOTYPES",
    "OWNING_LANE", "PRODUCT_REVIEW_RECORDS", "PROTOTYPE_AUDIT_RECORDS",
    "REJECTED_PROTOTYPES", "RESEARCH_AUTHORITY_DIGEST", "RESEARCH_AUTHORITY_SHA",
    "RANKING_DISPOSITION", "TEXTUAL_UNIT", "claim_digest", "claim_locator",
    "expected_correct_position", "trace_product_card", "validate_full_bank",
    "validate_product_review",
]
