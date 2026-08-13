# questions/__init__.py

from __future__ import annotations

import random

from .chapter1 import (
    all_chapter1_questions,
    easy_questions,
    easy_questions_v17_25,
    geography_questions,
    hard_questions,
    hard_questions_v17_25,
    linguistics_ch1_questions,
    linguistics_ch1_questions_2,
    linguistics_v17_25_questions,
    medium_questions,
    medium_questions_v17_25,
    nero_questions,
    practical_ch1_questions,
    practical_v17_25_questions,
)
from .intro import (
    intro_part1_questions,
    intro_part2_questions,
    intro_part3_questions,
)


_POOLS: dict[str, list] = {
    "easy": easy_questions + easy_questions_v17_25,
    "easy_p1": easy_questions,
    "easy_p2": easy_questions_v17_25,
    "medium": medium_questions + medium_questions_v17_25,
    "medium_p1": medium_questions,
    "medium_p2": medium_questions_v17_25,
    "hard": hard_questions + hard_questions_v17_25,
    "hard_p1": hard_questions,
    "hard_p2": hard_questions_v17_25,
    "practical_ch1": practical_ch1_questions + practical_v17_25_questions,
    "practical_p1": practical_ch1_questions,
    "practical_p2": practical_v17_25_questions,
    "linguistics_ch1": linguistics_ch1_questions,
    "linguistics_ch1_2": linguistics_ch1_questions_2,
    "linguistics_ch1_3": linguistics_v17_25_questions,
    "nero": nero_questions,
    "geography": geography_questions,
    "intro1": intro_part1_questions,
    "intro2": intro_part2_questions,
    "intro3": intro_part3_questions,
}


def _dedupe_pool(keys: list[str]) -> list:
    """Build a stable pool with one canonical occurrence of every explicit id."""
    seen: set[str] = set()
    result: list = []
    for key in keys:
        for question in _POOLS[key]:
            qid = str(question.get("id") or "").strip()
            if not qid or qid in seen:
                continue
            seen.add(qid)
            result.append(question)
    return result


# Casual random mode intentionally spans every learning category. It is not the
# source of truth for scored competitive modes.
_RANDOM_ALL_LEAF_KEYS = [
    "easy_p1",
    "easy_p2",
    "medium_p1",
    "medium_p2",
    "hard_p1",
    "hard_p2",
    "practical_p1",
    "practical_p2",
    "linguistics_ch1",
    "linguistics_ch1_2",
    "linguistics_ch1_3",
    "nero",
    "geography",
    "intro1",
    "intro2",
    "intro3",
]
_POOLS["random_all"] = _dedupe_pool(_RANDOM_ALL_LEAF_KEYS)


# Ranking eligibility is stricter than learning availability. These core-pool
# items depend on disputed dating/authorship reconstruction, imported historical
# context, or Greek claims still under dedicated source review. They remain
# available in their normal learning levels but cannot influence PvP/Challenge
# ranking until reviewed and explicitly released.
NON_COMPETITIVE_IDS = frozenset(
    {
        "easy_02",   # dating of 1 Peter
        "easy_03",   # Babylon = Rome / place of composition
        "easy_04",   # Silvanus as secretary/editor
        "easy_12",   # emperor depends on disputed dating
        "med_01",    # Babylon = Rome reconstruction
        "med_02",    # Neronian historical context
        "med_03",    # disputed semantic/theological reading of prognosis
        "med_13",    # corrupted/under-review diaspora lexical annotation
        "med_15",    # dating of 1 Peter
        "hard_02",   # Greek lexical item under dedicated review
        "hard_03",   # proposed Exodus 12:11 allusion
        "hard_11",   # dating of 1 Peter
        "hard_12",   # Greek lexical/rhetorical claim under dedicated review
        "hard_13",   # proposed Exodus 24 covenant background
    }
)

_COMPETITIVE_LEAF_KEYS = [
    "easy_p1",
    "easy_p2",
    "medium_p1",
    "medium_p2",
    "hard_p1",
    "hard_p2",
]


def _build_competitive_pool() -> list:
    return [
        question
        for question in _dedupe_pool(_COMPETITIVE_LEAF_KEYS)
        if str(question.get("id") or "").strip() not in NON_COMPETITIVE_IDS
    ]


COMPETITIVE_POOL = _build_competitive_pool()
_POOLS["competitive_all"] = COMPETITIVE_POOL
POOL_REGISTRY = _POOLS

# Production PvP samples only from this bank and must not append unrelated pools.
BATTLE_POOL = COMPETITIVE_POOL

# Per-difficulty slices use exactly the same eligibility policy.
CHALLENGE_POOLS: dict[str, list] = {
    "easy": [
        question
        for question in _POOLS["easy"]
        if str(question.get("id") or "").strip() not in NON_COMPETITIVE_IDS
    ],
    "medium": [
        question
        for question in _POOLS["medium"]
        if str(question.get("id") or "").strip() not in NON_COMPETITIVE_IDS
    ],
    "hard": [
        question
        for question in _POOLS["hard"]
        if str(question.get("id") or "").strip() not in NON_COMPETITIVE_IDS
    ],
}

_CHALLENGE_DISTRIBUTION = {
    "random20": (("easy", 6), ("medium", 6), ("hard", 8)),
    "hardcore20": (("easy", 4), ("medium", 4), ("hard", 12)),
}


def pick_competitive_challenge_questions(mode: str, *, rng=None) -> list:
    """Return exactly 20 unique ranking-eligible questions.

    ``rng`` may be a ``random.Random`` instance in tests. If one difficulty pool
    is unexpectedly short, the remainder is filled from the rest of the same
    ranking-eligible bank; it never falls back to excluded learning categories.
    """
    distribution = _CHALLENGE_DISTRIBUTION.get(mode)
    if distribution is None:
        raise ValueError(f"Неизвестный competitive Challenge mode: {mode!r}")

    source = rng or random
    selected: list = []
    seen: set[str] = set()

    for key, requested in distribution:
        candidates = [
            question
            for question in CHALLENGE_POOLS[key]
            if str(question.get("id") or "").strip() not in seen
        ]
        take = min(requested, len(candidates))
        for question in source.sample(candidates, take):
            qid = str(question.get("id") or "").strip()
            if not qid or qid in seen:
                continue
            seen.add(qid)
            selected.append(question)

    if len(selected) < 20:
        remainder = [
            question
            for question in COMPETITIVE_POOL
            if str(question.get("id") or "").strip() not in seen
        ]
        needed = 20 - len(selected)
        if len(remainder) < needed:
            raise ValueError(
                "Competitive question bank contains fewer than 20 unique questions"
            )
        for question in source.sample(remainder, needed):
            qid = str(question.get("id") or "").strip()
            seen.add(qid)
            selected.append(question)

    if len(selected) != 20 or len(seen) != 20:
        raise ValueError(
            "Competitive Challenge selection is not exactly 20 unique questions"
        )

    source.shuffle(selected)
    return selected


def get_pool_by_key(key: str) -> list:
    """Return a named pool and fail loudly on configuration typos."""
    try:
        return _POOLS[key]
    except KeyError:
        raise KeyError(
            f"Неизвестный pool_key: {key!r}. Доступные: {list(_POOLS.keys())}"
        )


__all__ = [
    "easy_questions",
    "easy_questions_v17_25",
    "medium_questions",
    "medium_questions_v17_25",
    "hard_questions",
    "hard_questions_v17_25",
    "nero_questions",
    "geography_questions",
    "practical_ch1_questions",
    "practical_v17_25_questions",
    "linguistics_ch1_questions",
    "linguistics_ch1_questions_2",
    "linguistics_v17_25_questions",
    "all_chapter1_questions",
    "intro_part1_questions",
    "intro_part2_questions",
    "intro_part3_questions",
    "POOL_REGISTRY",
    "NON_COMPETITIVE_IDS",
    "COMPETITIVE_POOL",
    "BATTLE_POOL",
    "CHALLENGE_POOLS",
    "pick_competitive_challenge_questions",
    "get_pool_by_key",
]
