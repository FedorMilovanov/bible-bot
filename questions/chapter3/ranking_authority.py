"""Explicit Chapter 3 ranking authority checkpoint.

This module pins the exact reviewed direct-text IDs that passed the prior
fail-closed ranking audit. It is authority only: root competitive/Battle/Challenge
composition is unchanged until a later gameplay PR consumes this list explicitly.
"""

from __future__ import annotations

CHAPTER3_RANKING_AUTHORIZED_IDS = frozenset(
    {
        "ch3_text_101",
        "ch3_text_102",
        "ch3_text_103",
        "ch3_text_104",
        "ch3_text_105",
        "ch3_text_106",
        "ch3_text_107",
        "ch3_text_108",
        "ch3_text_109",
        "ch3_text_110",
        "ch3_text_111",
        "ch3_text_112",
    }
)

CHAPTER3_RANKING_AUTHORITY = {
    qid: {
        "lane": "3:1-7",
        "claim_type": "text",
        "confidence": "high",
        "position": "neutral",
        "required_sources": ("sblgnt", "net_1p3_1_7"),
        "source_depth": {
            "sblgnt": "inspected_primary",
            "net_1p3_1_7": "inspected_passage",
        },
        "gameplay_admitted": False,
    }
    for qid in sorted(CHAPTER3_RANKING_AUTHORIZED_IDS)
}

__all__ = [
    "CHAPTER3_RANKING_AUTHORIZED_IDS",
    "CHAPTER3_RANKING_AUTHORITY",
]
