"""Canonical Chapter-5 release bank.

``bank_raw`` preserves the exact Agent-3 authoring output. This module performs
one release-only metadata narrowing step: each product card's source list is
reduced to the exact source IDs owned by its final Agent-1 Research claim, in
canonical claim-edge order. The projection may never add evidence that Agent 3
did not already cite and may never change wording, answers, or claim metadata.
"""
from __future__ import annotations

from copy import deepcopy

from ..research_handoff_v2 import CHAPTER5_RESEARCH_HANDOFF_V2
from .bank_raw import CHAPTER5_STAGING_QUESTIONS as _AGENT3_STAGING_QUESTIONS

CHAPTER5_STAGING_QUESTIONS = deepcopy(_AGENT3_STAGING_QUESTIONS)
SOURCE_ALIGNMENT_REMOVALS: dict[str, tuple[str, ...]] = {}

for _card in CHAPTER5_STAGING_QUESTIONS:
    _candidate_id = str(_card["research_candidate_id"])
    _research = CHAPTER5_RESEARCH_HANDOFF_V2.get(_candidate_id)
    if _research is None:
        raise ValueError(f"Chapter-5 card has no final Research claim: {_candidate_id}")
    _agent3_sources = tuple(str(source_id) for source_id in _card["sources"])
    _canonical_sources = tuple(str(source_id) for source_id in _research["source_ids"])
    if not set(_canonical_sources).issubset(_agent3_sources):
        missing = sorted(set(_canonical_sources) - set(_agent3_sources))
        raise ValueError(
            f"Chapter-5 release projection would add unreviewed evidence for "
            f"{_candidate_id}: {missing}"
        )
    _removed = tuple(
        source_id for source_id in _agent3_sources if source_id not in _canonical_sources
    )
    if _removed:
        SOURCE_ALIGNMENT_REMOVALS[_candidate_id] = _removed
    _card["sources"] = list(_canonical_sources)

if len(CHAPTER5_STAGING_QUESTIONS) != 72:
    raise ValueError("Chapter-5 canonical release bank must contain exactly 72 cards")
if len({str(card["id"]) for card in CHAPTER5_STAGING_QUESTIONS}) != 72:
    raise ValueError("Chapter-5 canonical release card IDs must be unique")

__all__ = ["CHAPTER5_STAGING_QUESTIONS", "SOURCE_ALIGNMENT_REMOVALS"]
