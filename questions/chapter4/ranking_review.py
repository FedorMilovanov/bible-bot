"""Explicit independent product ranking review for the w3q_123 discrepancy."""

from __future__ import annotations

from types import MappingProxyType

from .research_handoff import (
    RESEARCH_AUTHORITY_DIGEST_SHA256,
    RESEARCH_AUTHORITY_SHA,
    RESEARCH_HANDOFF_V2,
    RESEARCH_REPOSITORY,
)

_claim = RESEARCH_HANDOFF_V2["w3q_123"]

W3Q123_RANKING_REVIEW = MappingProxyType({
    "schema_version": 2,
    "ranking_review_id": "ch4rankv2_w3q_123_no_admission",
    "research_repository": RESEARCH_REPOSITORY,
    "research_authority_sha": RESEARCH_AUTHORITY_SHA,
    "research_authority_digest_sha256": RESEARCH_AUTHORITY_DIGEST_SHA256,
    "research_claim_id": "w3q_123",
    "research_effective_claim_digest": _claim["research_effective_claim_digest"],
    "source_ids": tuple(_claim["source_ids"]),
    "claim_inspection_edge_ids": tuple(_claim["claim_inspection_edge_ids"]),
    "research_position": _claim["position"],
    "research_confidence": _claim["confidence"],
    "research_claim_type": _claim["claim_type"],
    "research_competitive_candidate": False,
    "research_ranking_discrepancy_candidate": True,
    "product_ranking_decision": "NO_RANKING_ADMISSION",
    "reviewer": MappingProxyType({
        "reviewer_id": "chapter4-product-ranking-review-v2-agent",
        "reviewer_role": "independent_product_ranking_reviewer",
    }),
    "reasons": (
        "Research authority remains READY_NONCOMPETITIVE with competitive_candidate=false.",
        "The claim belongs to the genuine 1 Peter 4:16 textual-variation unit ὀνόματι / μέρει.",
        "Kok/de Winter is scholarly exposition of ECM/CBGM, not direct product-side dECM witness-table readback.",
        "No Chapter-3-style separate product ranking authority exists for Chapter 4.",
        "Chapter 4 textual criticism remains noncompetitive by product policy.",
    ),
    "competitive_pool_admission": False,
    "battle_admission": False,
    "challenge_admission": False,
})

if W3Q123_RANKING_REVIEW["product_ranking_decision"] != "NO_RANKING_ADMISSION":
    raise ValueError("w3q_123 must fail closed without separate Chapter-3-style authority")

__all__ = ["W3Q123_RANKING_REVIEW"]
