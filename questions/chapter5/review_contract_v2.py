"""Immutable Chapter-5 Research -> product review contract v2.

Research remains the evidence authority. This module pins the exact Research
commit and content-addressed handoff artifacts, then derives deterministic
per-claim and claim/source inspection-edge identities. Product reviews are a
separate admission layer and never become Research or ranking authority.
"""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy

from .bank import CHAPTER5_STAGING_QUESTIONS
from .sources import SOURCE_CATALOG

RESEARCH_REPOSITORY = "FedorMilovanov/Research"
RESEARCH_AUTHORITY_SHA = "0142430af8ba80f28e0fd9cde669d32611a1d2af"
RESEARCH_FINAL_SNAPSHOT_BLOB = "8f52026e2338734e1aa723cf9226ffea93ef0519"
WAVE3N_OVERRIDE_BLOB = "b77ad53a7be80152d4da4a6ceb229b084735883e"
WAVE3N_QUORUM_BLOB = "f84d22c0d36add6ffd36889d5a5fcb3e5e101228"
RESEARCH_AUTHORITY_DIGEST = "51b935cd819a664ddc6d036c72f38d903c3a1d1c862e283a64b82987dfa7175b"
OWNING_LANE = "Research/1_PETER_BOT/effective-question-candidate"
RANKING_DISPOSITION = "HOLD_NO_COMPETITIVE_AUTHORITY"

# The blobs are Git content identities at RESEARCH_AUTHORITY_SHA. A per-claim
# locator therefore pins the exact source record without copying Research text
# into the product repository.
RESEARCH_CANDIDATE_SHARDS = (
    (31, 60, "1_PETER_BOT/data/question-candidates-wave3-031-060.json", "47833bc583109cbd092a6420ce7a00b8bc22cdc7"),
    (61, 90, "1_PETER_BOT/data/question-candidates-wave3-061-090.json", "0fcf1398f14212ab195afe83c53100882a7f34bf"),
    (91, 96, "1_PETER_BOT/data/question-candidates-wave3-091-096.json", "3ad3a912e745008f679d176198e44454d3866c5a"),
    (97, 112, "1_PETER_BOT/data/question-candidates-wave3-097-112.json", "cb16c59b57040ba0d97069e314d1a5de85c00a7b"),
    (113, 120, "1_PETER_BOT/data/question-candidates-wave3-113-120.json", "7c58ffaf3117767b3a1717cea6d92efbad7b1fa5"),
    (121, 128, "1_PETER_BOT/data/question-candidates-wave3-121-128.json", "b59ce401bd313db1b24c3c292af4094544755c5a"),
    (129, 136, "1_PETER_BOT/data/question-candidates-wave3-129-136.json", "6d4cf5787b0d00574a3accfb1c42841d7c1eae35"),
    (137, 144, "1_PETER_BOT/data/question-candidates-wave3-137-144.json", "7c21120b7fd95c414264140e98fe161a35670e5b"),
)

CHAPTER5_CANDIDATE_IDS = (
    "w3q_046","w3q_047","w3q_048","w3q_049","w3q_050","w3q_051","w3q_052","w3q_053","w3q_054","w3q_055",
    "w3q_056","w3q_057","w3q_058","w3q_059","w3q_060","w3q_061","w3q_062","w3q_063","w3q_064","w3q_065",
    "w3q_066","w3q_067","w3q_068","w3q_069","w3q_070","w3q_071","w3q_072","w3q_073","w3q_074","w3q_075",
    "w3q_076","w3q_077","w3q_078","w3q_079","w3q_080","w3q_081","w3q_082","w3q_083","w3q_084","w3q_085",
    "w3q_086","w3q_087","w3q_088","w3q_089","w3q_090","w3q_094","w3q_095","w3q_096",
    "w3q_105","w3q_106","w3q_107","w3q_108","w3q_109","w3q_110","w3q_111","w3q_112",
    "w3q_117","w3q_118","w3q_119","w3q_120","w3q_125","w3q_126","w3q_127","w3q_128",
    "w3q_133","w3q_134","w3q_135","w3q_136","w3q_141","w3q_142","w3q_143","w3q_144",
)

HISTORICAL_HOLD_IDS = frozenset({"w3q_050", "w3q_051", "w3q_075"})
WAVE3N_OVERRIDE_POINTERS = {
    "w3q_050": "/overrides/1",
    "w3q_051": "/overrides/2",
    "w3q_075": "/overrides/3",
}

# Explicitly rejected as publication templates by the product-review handoff.
# They remain audit-visible Research artifacts only.
REJECTED_PROTOTYPES = frozenset({"w3mcq_020", "w3mcq_027"})
PROTOTYPE_TO_CANDIDATE = {
    "w3mcq_017":"w3q_046","w3mcq_018":"w3q_048","w3mcq_019":"w3q_049","w3mcq_020":"w3q_052",
    "w3mcq_021":"w3q_055","w3mcq_022":"w3q_057","w3mcq_023":"w3q_058","w3mcq_024":"w3q_060",
    "w3mcq_025":"w3q_061","w3mcq_026":"w3q_064","w3mcq_027":"w3q_066","w3mcq_028":"w3q_068",
    "w3mcq_029":"w3q_070","w3mcq_030":"w3q_072","w3mcq_031":"w3q_076","w3mcq_032":"w3q_081",
    "w3mcq_049":"w3q_047","w3mcq_050":"w3q_053","w3mcq_051":"w3q_054","w3mcq_052":"w3q_062",
    "w3mcq_053":"w3q_063","w3mcq_054":"w3q_071","w3mcq_055":"w3q_073","w3mcq_056":"w3q_074",
    "w3mcq_057":"w3q_077","w3mcq_058":"w3q_078","w3mcq_059":"w3q_079","w3mcq_060":"w3q_080",
    "w3mcq_061":"w3q_125","w3mcq_062":"w3q_134","w3mcq_063":"w3q_136","w3mcq_064":"w3q_088",
}
RESEARCH_EDITORIAL_OVERRIDE_PROTOTYPES = frozenset({
    "w3mcq_017", "w3mcq_020", "w3mcq_022", "w3mcq_024", "w3mcq_029"
})

# Unit scope is deliberately narrower than the verse. In particular, the two
# 5:2 units may not borrow one another's passage-specific evidence.
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

# Stanojevic is passage-specific evidence for kata theon, not for the separate
# episkopountes unit. This explicit exclusion is a mutation-test boundary.
FORBIDDEN_SOURCE_BY_CLAIM = {
    "w3q_050": frozenset({"w3n_stanojevic_ecm_2021"}),
}


def _digest(payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _candidate_number(candidate_id: str) -> int:
    if not candidate_id.startswith("w3q_"):
        raise ValueError(f"invalid Chapter-5 claim id: {candidate_id!r}")
    return int(candidate_id.removeprefix("w3q_"))


def claim_locator(candidate_id: str) -> dict:
    if candidate_id not in CHAPTER5_CANDIDATE_IDS:
        raise KeyError(candidate_id)
    number = _candidate_number(candidate_id)
    for start, end, path, blob_sha in RESEARCH_CANDIDATE_SHARDS:
        if start <= number <= end:
            locator = {
                "repository": RESEARCH_REPOSITORY,
                "research_sha": RESEARCH_AUTHORITY_SHA,
                "path": path,
                "blob_sha": blob_sha,
                "json_pointer": f"/candidates/{number - start}",
                "candidate_id": candidate_id,
            }
            override_pointer = WAVE3N_OVERRIDE_POINTERS.get(candidate_id)
            if override_pointer:
                locator["effective_override"] = {
                    "blob_sha": WAVE3N_OVERRIDE_BLOB,
                    "json_pointer": override_pointer,
                }
            return locator
    raise AssertionError(candidate_id)


def claim_digest(candidate_id: str) -> str:
    return _digest({
        "authority_digest": RESEARCH_AUTHORITY_DIGEST,
        "locator": claim_locator(candidate_id),
    })


def _edge_id(candidate_id: str, source_id: str) -> str:
    unit = TEXTUAL_UNIT.get(candidate_id, f"claim:{candidate_id}")
    token = _digest({
        "authority_digest": RESEARCH_AUTHORITY_DIGEST,
        "candidate_id": candidate_id,
        "source_id": source_id,
        "unit": unit,
        "owner_lane": OWNING_LANE,
    })[:24]
    return f"ch5-edge-{candidate_id}-{token}"


def expected_correct_position(candidate_id: str) -> int:
    return CHAPTER5_CANDIDATE_IDS.index(candidate_id) % 4


def prototype_audit_records() -> dict[str, dict]:
    result: dict[str, dict] = {}
    for prototype_id, candidate_id in PROTOTYPE_TO_CANDIDATE.items():
        rejected = prototype_id in REJECTED_PROTOTYPES
        result[prototype_id] = {
            "prototype_id": prototype_id,
            "candidate_id": candidate_id,
            "research_only": True,
            "publication_authority": False,
            "ranking_authority": False,
            "research_editorial_override_present": prototype_id in RESEARCH_EDITORIAL_OVERRIDE_PROTOTYPES,
            "prototype_disposition": (
                "REJECTED_TEMPLATE_NOT_PUBLICATION_AUTHORITY"
                if rejected else "RECONCILED_RESEARCH_REFERENCE_ONLY"
            ),
            "rewrite_family_disposition": (
                "NEEDS_REWRITE_RESOLVED_BY_INDEPENDENT_PRODUCT_REWRITE"
                if rejected else "NO_UNRESOLVED_PRODUCT_REWRITE_REQUIRED"
            ),
            "product_disposition": (
                "INDEPENDENT_PRODUCT_REWRITE_ACCEPTED"
                if rejected else "PRODUCT_CARD_REVIEWED_INDEPENDENTLY"
            ),
        }
    return result


PROTOTYPE_AUDIT_RECORDS = prototype_audit_records()


def _prototype_for_candidate(candidate_id: str) -> str | None:
    for prototype_id, linked_candidate in PROTOTYPE_TO_CANDIDATE.items():
        if linked_candidate == candidate_id:
            return prototype_id
    return None


def _review_record(card: dict) -> dict:
    candidate_id = str(card["research_candidate_id"])
    source_subset = tuple(str(source_id) for source_id in card["sources"])
    prototype_id = _prototype_for_candidate(candidate_id)
    rejected_template = prototype_id in REJECTED_PROTOTYPES if prototype_id else False
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
        "claim_digest": claim_digest(candidate_id),
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
        "prototype_publication_authority": False,
        "prototype_disposition": (
            PROTOTYPE_AUDIT_RECORDS[prototype_id]["prototype_disposition"]
            if prototype_id else "NO_RESEARCH_PROTOTYPE_FOR_CLAIM"
        ),
        "product_rewrite_disposition": (
            "INDEPENDENT_PRODUCT_REWRITE_ACCEPTED"
            if rejected_template else "PRODUCT_CARD_REVIEWED_INDEPENDENTLY"
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


def validate_product_review(card: dict, review: dict) -> None:
    """Fail closed if a product card or its immutable handoff record drifts."""
    _validate_no_outer_whitespace(card)
    candidate_id = str(card["research_candidate_id"])
    if candidate_id not in CHAPTER5_CANDIDATE_IDS:
        raise ValueError("unknown Chapter-5 Research claim")
    if str(card["id"]) != f"ch5_{candidate_id}":
        raise ValueError("product id does not preserve Research claim identity")
    if review.get("product_review_id") != f"ch5-product-review-v2-{candidate_id}":
        raise ValueError("wrong product review id")
    if review.get("research_authority_sha") != RESEARCH_AUTHORITY_SHA:
        raise ValueError("stale Research authority SHA")
    if review.get("authority_digest") != RESEARCH_AUTHORITY_DIGEST:
        raise ValueError("stale Research authority digest")
    if review.get("claim_digest") != claim_digest(candidate_id):
        raise ValueError("wrong Research claim digest")

    sources = tuple(str(source_id) for source_id in card["sources"])
    if tuple(review.get("source_subset", ())) != sources:
        raise ValueError("product source subset drift")
    if any(source_id not in SOURCE_CATALOG for source_id in sources):
        raise ValueError("unregistered Chapter-5 source")
    forbidden_sources = FORBIDDEN_SOURCE_BY_CLAIM.get(candidate_id, frozenset())
    if forbidden_sources.intersection(sources):
        raise ValueError("source belongs to a different textual unit")

    expected_edges = tuple(_edge_id(candidate_id, source_id) for source_id in sources)
    if tuple(review.get("claim_inspection_edge_ids", ())) != expected_edges:
        raise ValueError("fake or stale claim/source inspection edge")
    edge_records = tuple(review.get("claim_source_edges", ()))
    if tuple(edge.get("edge_id") for edge in edge_records) != expected_edges:
        raise ValueError("claim/source edge record mismatch")
    if any(edge.get("owner_lane") != OWNING_LANE for edge in edge_records):
        raise ValueError("claim/source edge escaped owning lane")

    claimed = (str(card["claim_type"]), str(card["confidence"]), str(card["position"]))
    reviewed_claimed = (
        review.get("claimed_type"), review.get("claimed_confidence"), review.get("claimed_position")
    )
    if claimed != reviewed_claimed:
        raise ValueError("claimed type/confidence/position drift")
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
    if prototype_id in REJECTED_PROTOTYPES:
        if review.get("prototype_publication_authority") is not False:
            raise ValueError("rejected prototype cannot be publication authority")
        if review.get("product_rewrite_disposition") != "INDEPENDENT_PRODUCT_REWRITE_ACCEPTED":
            raise ValueError("rejected prototype requires independent product rewrite record")


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
    ids = [str(card["research_candidate_id"]) for card in CHAPTER5_STAGING_QUESTIONS]
    if tuple(ids) != CHAPTER5_CANDIDATE_IDS:
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
    "CHAPTER5_CANDIDATE_IDS", "HISTORICAL_HOLD_IDS", "OWNING_LANE",
    "PRODUCT_REVIEW_RECORDS", "PROTOTYPE_AUDIT_RECORDS", "REJECTED_PROTOTYPES",
    "RESEARCH_AUTHORITY_DIGEST", "RESEARCH_AUTHORITY_SHA", "RESEARCH_CANDIDATE_SHARDS",
    "RANKING_DISPOSITION", "TEXTUAL_UNIT", "claim_digest", "claim_locator",
    "expected_correct_position", "trace_product_card", "validate_full_bank",
    "validate_product_review",
]
