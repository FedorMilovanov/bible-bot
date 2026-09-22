"""Human-reviewed cognitive difficulty for the exposed Chapter-1/TMS courses.

Difficulty is about the task a learner must perform, not about whether a card
contains Greek, history, or long words.

* base: locate, recall, or recognize one direct textual fact;
* core: connect clauses, follow an argument, or explain a contextual relation;
* advanced: discriminate serious exegetical/historical alternatives, work with
  syntax/semantics, intertextual evidence, or multi-step theological synthesis.

The map is deliberately explicit by stable question ID.  It does not infer a
level from claim_type and it does not relabel all cards in a pool just because
the pool happens to be named "medium" or "hard".
"""
from __future__ import annotations

from copy import deepcopy

LEVEL_REVIEW_ID = "chapter1-cognitive-levels-2026-09-v1"
VALID_LEVELS = frozenset({"base", "core", "advanced"})


_BASE_IDS = frozenset(
    {
        # The two Easy courses are intentionally the entry-level textual layer.
        "easy_01", "easy_02", "easy_03", "easy_04", "easy_05",
        "easy_06", "easy_07", "easy_08", "easy_09", "easy_10",
        "easy_11", "easy_12", "easy_13", "easy_14", "easy_15",
        "easy_es2_01", "easy_es2_02", "easy_es2_03", "easy_es2_04", "easy_es2_05",
        "easy_es2_06", "easy_es2_07", "easy_es2_08", "easy_es2_09", "easy_es2_10",
        "easy17_01", "easy17_02", "easy17_03", "easy17_04", "easy17_05",
        "easy17_06", "easy17_07", "easy17_08", "easy17_09", "easy17_10",
        "easy17_11", "easy17_12", "easy17_13", "easy17_14", "easy17_15",
        "easy17_es2_01", "easy17_es2_02", "easy17_es2_03", "easy17_es2_04",
        "easy17_es2_05", "easy17_es2_06", "easy17_es2_07", "easy17_es2_08",
        "easy17_es2_09", "easy17_es2_10",

        # Medium-labelled cards that are actually one-step recall/recognition.
        "med_02", "med_04", "med_05", "med_08", "med_09", "med_10", "med_11",
        "med_12",
        "med17_07", "med17_09", "med17_11", "med17_12",

        # Hard-labelled cards whose present cognitive task is still direct recall.
        "hard_04", "hard_05", "hard_07",

        # TMS entry layer.
        "tms1_easy_01", "tms1_easy_02", "tms1_easy_03",
    }
)

_CORE_IDS = frozenset(
    {
        # Medium p1: contextual relations and argument tracing.
        "med_06", "med_07", "med_13", "med_14",
        "med_es2_01", "med_es2_02", "med_es2_03", "med_es2_04", "med_es2_05",
        "med_es2_06", "med_es2_07", "med_es2_08", "med_es2_09", "med_es2_10",

        # Medium p2.
        "med17_01", "med17_02", "med17_03", "med17_05", "med17_06", "med17_08",
        "med17_10", "med17_13", "med17_14",
        "med17_es2_01", "med17_es2_02", "med17_es2_03", "med17_es2_04",
        "med17_es2_05", "med17_es2_06", "med17_es2_07", "med17_es2_08",
        "med17_es2_09", "med17_es2_10",

        # Hard p1 cards that are substantial but do not yet require advanced
        # evidence discrimination.
        "hard_01", "hard_02", "hard_06", "hard_08", "hard_09", "hard_10",
        "hard_14", "hard_15",
        "hard_es2_01", "hard_es2_02", "hard_es2_03", "hard_es2_04",
        "hard_es2_05", "hard_es2_06", "hard_es2_07", "hard_es2_09",
        "hard_es2_10",

        # Hard p2 core exegesis.
        "hard17_02", "hard17_03", "hard17_05", "hard17_10", "hard17_11",
        "hard17_12", "hard17_13", "hard17_15",
        "hard17_es2_01", "hard17_es2_03", "hard17_es2_04", "hard17_es2_05",
        "hard17_es2_07", "hard17_es2_08", "hard17_es2_10",

        # TMS middle/application layer.
        "tms1_med_01", "tms1_med_02", "tms1_med_03", "tms1_med_04",
        "tms1_app_01", "tms1_app_02", "tms1_app_03",
    }
)

_ADVANCED_IDS = frozenset(
    {
        # Medium-labelled cards whose task really does require historical,
        # lexical, or structural adjudication.
        "med_01", "med_03", "med_15",
        "med17_04", "med17_15",

        # Hard p1: intertext, historical reconstruction, Greek semantics, and
        # multi-step theological synthesis.
        "hard_03", "hard_11", "hard_12", "hard_13", "hard_es2_08",
        "hard_deep_01", "hard_deep_02", "hard_deep_03", "hard_deep_04",
        "hard_deep_05", "hard_deep_11", "hard_deep_12", "hard_deep_13",
        "hard_deep_14", "hard_deep_15",

        # Hard p2.
        "hard17_01", "hard17_04", "hard17_06", "hard17_07", "hard17_08",
        "hard17_09", "hard17_14",
        "hard17_es2_02", "hard17_es2_06", "hard17_es2_09",
        "hard_deep_06", "hard_deep_07", "hard_deep_08", "hard_deep_09",
        "hard_deep_10", "hard_deep_16", "hard_deep_17", "hard_deep_18",
        "hard_deep_19", "hard_deep_20",

        # TMS hard layer after the seminary-depth rewrite.
        "tms1_hard_01", "tms1_hard_02", "tms1_hard_03", "tms1_hard_04",
        "tms1_hard_05",
    }
)

REVIEWED_LEVEL_BY_ID = {
    **{question_id: "base" for question_id in _BASE_IDS},
    **{question_id: "core" for question_id in _CORE_IDS},
    **{question_id: "advanced" for question_id in _ADVANCED_IDS},
}

# These are the only pools whose user-facing product names currently make a
# difficulty promise. TMS is included because its own easy/medium/hard sublayer
# is explicitly authored in the stable IDs.
REVIEWED_LEVEL_POOLS = frozenset(
    {
        "easy_p1", "easy_p2",
        "medium_p1", "medium_p2",
        "hard_p1", "hard_p2",
        "tms_deep",
    }
)


def apply_reviewed_level(item: dict, *, pool_key: str) -> dict:
    """Attach a reviewed cognitive level without mutating the input card."""
    reviewed = deepcopy(item)
    if pool_key not in REVIEWED_LEVEL_POOLS:
        return reviewed

    question_id = str(reviewed.get("id") or "").strip()
    level = REVIEWED_LEVEL_BY_ID.get(question_id)
    if level is None:
        raise ValueError(
            f"{pool_key}/{question_id}: exposed difficulty course lacks reviewed level"
        )
    if level not in VALID_LEVELS:
        raise ValueError(f"{question_id}: invalid reviewed level {level!r}")
    reviewed["level"] = level
    return reviewed


def reviewed_level_counts() -> dict[str, int]:
    return {
        level: sum(1 for value in REVIEWED_LEVEL_BY_ID.values() if value == level)
        for level in ("base", "core", "advanced")
    }


def _assert_review_map() -> None:
    overlap = (
        (_BASE_IDS & _CORE_IDS)
        | (_BASE_IDS & _ADVANCED_IDS)
        | (_CORE_IDS & _ADVANCED_IDS)
    )
    if overlap:
        raise ValueError(f"difficulty review assigns multiple levels: {sorted(overlap)}")
    if len(REVIEWED_LEVEL_BY_ID) != 185:
        raise ValueError(
            "difficulty review must cover exactly 185 Chapter-1/TMS cards; "
            f"got {len(REVIEWED_LEVEL_BY_ID)}"
        )
    if reviewed_level_counts() != {"base": 68, "core": 72, "advanced": 45}:
        raise ValueError(f"unexpected difficulty distribution: {reviewed_level_counts()}")


_assert_review_map()

__all__ = [
    "LEVEL_REVIEW_ID",
    "REVIEWED_LEVEL_BY_ID",
    "REVIEWED_LEVEL_POOLS",
    "VALID_LEVELS",
    "apply_reviewed_level",
    "reviewed_level_counts",
]
