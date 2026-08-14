"""Reviewed Challenge difficulty taxonomy for authorized Chapter 3 text cards.

Taxonomy is editorial authority only. This module does not change CHALLENGE_POOLS.
All twelve source-reviewed ranking-authorized cards are classified exactly once;
none is forced into `hard` merely to create a symmetric distribution.
"""

from __future__ import annotations

from .ranking_authority import CHAPTER3_RANKING_AUTHORIZED_IDS

CHAPTER3_CHALLENGE_TAXONOMY = {
    "easy": (
        "ch3_text_101",
        "ch3_text_102",
        "ch3_text_106",
        "ch3_text_108",
        "ch3_text_109",
        "ch3_text_111",
    ),
    "medium": (
        "ch3_text_103",
        "ch3_text_104",
        "ch3_text_105",
        "ch3_text_107",
        "ch3_text_110",
        "ch3_text_112",
    ),
    "hard": (),
}

CHAPTER3_CHALLENGE_RATIONALE = {
    "ch3_text_101": "Direct scope identification: own husbands rather than a universal gender relation.",
    "ch3_text_102": "Direct quantifier boundary: some husbands, not all husbands or all marriages.",
    "ch3_text_106": "Single-clause identification of God's stated valuation of the quiet/gentle spirit.",
    "ch3_text_108": "Direct identification of the two actions explicitly attached to Sarah in 3:6.",
    "ch3_text_109": "Direct identification of the closing paired actions in 3:6.",
    "ch3_text_111": "Direct identification of the wife's co-heir relation to the grace of life.",
    "ch3_text_103": "Requires connecting the winning purpose with the observed conduct across 3:1-2.",
    "ch3_text_104": "Requires retaining a three-item enumeration while rejecting plausible neighboring adornment terms.",
    "ch3_text_105": "Requires tracking the explicit 3:3-to-3:4 outer/inner contrast rather than one isolated phrase.",
    "ch3_text_107": "Requires distinguishing the class description of holy women from Sarah-specific and husband-directed neighboring statements.",
    "ch3_text_110": "Requires identifying the paired positive husband obligations while separating them from nearby wife-directed clauses.",
    "ch3_text_112": "Requires recognizing the final purpose/result relation concerning hindered prayers in the husband instruction.",
}


def taxonomy_ids() -> frozenset[str]:
    return frozenset(qid for ids in CHAPTER3_CHALLENGE_TAXONOMY.values() for qid in ids)


if taxonomy_ids() != CHAPTER3_RANKING_AUTHORIZED_IDS:
    missing = sorted(CHAPTER3_RANKING_AUTHORIZED_IDS - taxonomy_ids())
    extra = sorted(taxonomy_ids() - CHAPTER3_RANKING_AUTHORIZED_IDS)
    raise ValueError(
        "Chapter 3 Challenge taxonomy must classify the explicit ranking authority exactly: "
        f"missing={missing}, extra={extra}"
    )

if set(CHAPTER3_CHALLENGE_RATIONALE) != set(CHAPTER3_RANKING_AUTHORIZED_IDS):
    raise ValueError("Every authorized Chapter 3 Challenge card requires an editorial rationale")

__all__ = [
    "CHAPTER3_CHALLENGE_RATIONALE",
    "CHAPTER3_CHALLENGE_TAXONOMY",
    "taxonomy_ids",
]
