"""Fail-closed ranking-readiness audit for the reviewed Chapter 3 bank.

This module does not admit any Chapter-3 card to root ranking pools. It evaluates
existing reviewed `competitive=True` candidate metadata against the repository's
current structural ranking policy plus a stricter lane-local source-depth gate.
"""

from __future__ import annotations

from . import CHAPTER3_SOURCE_CATALOGS
from .reviewed import (
    CHAPTER3_RANKING_CANDIDATE_IDS,
    CHAPTER3_REVIEWED_LANE_BY_ID,
    CHAPTER3_REVIEWED_QUESTIONS,
)
from ..ranking_policy import ranking_eligible

# Status vocabularies are intentionally lane-schema aware. A status not listed
# here fails closed rather than being guessed into readiness.
CLAIM_READY_SOURCE_STATUSES = frozenset(
    {
        # Agent A / 3:1-7 (`evidence_status`)
        "inspected_primary",
        "inspected_passage",
        "inspected_entry",
        "inspected_full_text",
        # Agent B / 3:8-12 (`inspection_level`)
        "primary_text_inspected",
        "primary_data_inspected",
        "entry_inspected",
        "full_text_official_pdf",
        "relevant_section_inspected",
        # Agent C / 3:13-17 (`inspection_status`)
        "relevant_article_text_inspected",
        "relevant_case_study_inspected_not_author_independent_from_2002",
        "relevant_3_15_comment_inspected",
    }
)

LIMITED_SOURCE_STATUSES = frozenset(
    {
        # A
        "inspected_abstract_only",
        "bibliographic_only",
        "bibliographic_toc_only",
        # B
        "publisher_abstract_only",
        # C
        "bibliographic_control_only",
        "metadata_preview_only_not_claim_evidence",
        "edition_metadata_inspected",
        "abstract_only",
        # D
        "publisher_abstract_inspected",
        "metadata_only",
    }
)

# Root-shared primary work identity is allowed only when the lane intentionally
# relies on the canonical SBLGNT text rather than declaring its own duplicate
# record. No commentary/lexicon gets this shortcut.
ROOT_PRIMARY_SOURCE_IDS = frozenset({"sblgnt"})

_STATUS_KEYS = (
    "evidence_status",
    "inspection_level",
    "inspection_status",
    "inspection_scope",
)


def _source_status(metadata: dict) -> str | None:
    for key in _STATUS_KEYS:
        value = str(metadata.get(key) or "").strip()
        if value:
            return value
    return None


def _audit_source(lane: str, source_id: str) -> tuple[bool, str]:
    local = CHAPTER3_SOURCE_CATALOGS[lane]
    if source_id not in local:
        if source_id in ROOT_PRIMARY_SOURCE_IDS:
            return True, "root_primary_canonical"
        return False, "root_only_without_lane_claim_inspection"

    status = _source_status(local[source_id])
    if status in CLAIM_READY_SOURCE_STATUSES:
        return True, status
    if status in LIMITED_SOURCE_STATUSES:
        return False, f"limited:{status}"
    if status is None:
        return False, "missing_inspection_status"
    return False, f"unknown_inspection_status:{status}"


def audit_ranking_candidate(item: dict) -> tuple[bool, tuple[str, ...]]:
    """Return readiness and stable fail-closed reasons for one reviewed card."""
    reasons: list[str] = []

    if item["id"] not in CHAPTER3_RANKING_CANDIDATE_IDS:
        reasons.append("not_reviewed_competitive_candidate")
        return False, tuple(reasons)

    if not ranking_eligible(item):
        reasons.append("fails_root_structural_ranking_policy")

    lane = CHAPTER3_REVIEWED_LANE_BY_ID[item["id"]]
    for source_id in item.get("sources") or []:
        ready, status = _audit_source(lane, source_id)
        if not ready:
            reasons.append(f"source:{source_id}:{status}")

    if not item.get("sources"):
        reasons.append("no_sources")

    return not reasons, tuple(reasons)


_REVIEWED_BY_ID = {item["id"]: item for item in CHAPTER3_REVIEWED_QUESTIONS}

CHAPTER3_RANKING_AUDIT = {
    qid: audit_ranking_candidate(_REVIEWED_BY_ID[qid])
    for qid in sorted(CHAPTER3_RANKING_CANDIDATE_IDS)
}

CHAPTER3_RANKING_READY_IDS = frozenset(
    qid
    for qid, (ready, _) in CHAPTER3_RANKING_AUDIT.items()
    if ready
)

CHAPTER3_RANKING_HOLD_REASONS = {
    qid: reasons
    for qid, (ready, reasons) in CHAPTER3_RANKING_AUDIT.items()
    if not ready
}

__all__ = [
    "CHAPTER3_RANKING_AUDIT",
    "CHAPTER3_RANKING_HOLD_REASONS",
    "CHAPTER3_RANKING_READY_IDS",
    "CLAIM_READY_SOURCE_STATUSES",
    "LIMITED_SOURCE_STATUSES",
    "ROOT_PRIMARY_SOURCE_IDS",
    "audit_ranking_candidate",
]
