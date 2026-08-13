# questions/__init__.py

from __future__ import annotations

import random

from .chapter1 import (
    easy_questions as _raw_easy_p1,
    easy_questions_v17_25 as _raw_easy_p2,
    geography_questions as _raw_geography,
    hard_questions as _raw_hard_p1,
    hard_questions_v17_25 as _raw_hard_p2,
    linguistics_ch1_questions as _raw_linguistics_1,
    linguistics_ch1_questions_2 as _raw_linguistics_2,
    linguistics_v17_25_questions as _raw_linguistics_3,
    medium_questions as _raw_medium_p1,
    medium_questions_v17_25 as _raw_medium_p2,
    nero_questions as _raw_nero,
    practical_ch1_questions as _raw_practical_p1,
    practical_v17_25_questions as _raw_practical_p2,
)
from .content_truth import (
    RANKING_QUARANTINE_IDS,
    SOURCE_CATALOG,
    curate_pool,
)
from .intro import (
    intro_part1_questions as _raw_intro1,
    intro_part2_questions as _raw_intro2,
    intro_part3_questions as _raw_intro3,
)


def _canonical(raw: list[dict], key: str) -> list[dict]:
    return curate_pool(raw, pool_key=key)


# Production-facing canonical leaf pools. Raw chapter1.py/intro.py remain an
# authoring/migration corpus only; handlers and APIs consume these curated copies.
easy_questions = _canonical(_raw_easy_p1, "easy_p1")
easy_questions_v17_25 = _canonical(_raw_easy_p2, "easy_p2")
medium_questions = _canonical(_raw_medium_p1, "medium_p1")
medium_questions_v17_25 = _canonical(_raw_medium_p2, "medium_p2")
hard_questions = _canonical(_raw_hard_p1, "hard_p1")
hard_questions_v17_25 = _canonical(_raw_hard_p2, "hard_p2")
practical_ch1_questions = _canonical(_raw_practical_p1, "practical_p1")
practical_v17_25_questions = _canonical(_raw_practical_p2, "practical_p2")
linguistics_ch1_questions = _canonical(_raw_linguistics_1, "linguistics_ch1")
linguistics_ch1_questions_2 = _canonical(_raw_linguistics_2, "linguistics_ch1_2")
linguistics_v17_25_questions = _canonical(_raw_linguistics_3, "linguistics_ch1_3")
nero_questions = _canonical(_raw_nero, "nero")
geography_questions = _canonical(_raw_geography, "geography")
intro_part1_questions = _canonical(_raw_intro1, "intro1")
intro_part2_questions = _canonical(_raw_intro2, "intro2")
intro_part3_questions = _canonical(_raw_intro3, "intro3")


_POOLS: dict[str, list[dict]] = {
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


def _dedupe_pool(keys: list[str]) -> list[dict]:
    """Build a stable pool with one canonical occurrence of every explicit id."""
    seen: set[str] = set()
    result: list[dict] = []
    for key in keys:
        for question in _POOLS[key]:
            qid = str(question.get("id") or "").strip()
            if not qid or qid in seen:
                continue
            seen.add(qid)
            result.append(question)
    return result


_CHAPTER1_LEAF_KEYS = [
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
]
all_chapter1_questions = _dedupe_pool(_CHAPTER1_LEAF_KEYS)

_RANDOM_ALL_LEAF_KEYS = _CHAPTER1_LEAF_KEYS + ["intro1", "intro2", "intro3"]
_POOLS["random_all"] = _dedupe_pool(_RANDOM_ALL_LEAF_KEYS)


_COMPETITIVE_LEAF_KEYS = [
    "easy_p1",
    "easy_p2",
    "medium_p1",
    "medium_p2",
    "hard_p1",
    "hard_p2",
]


def _build_competitive_pool() -> list[dict]:
    return [
        question
        for question in _dedupe_pool(_COMPETITIVE_LEAF_KEYS)
        if question.get("competitive") is True
    ]


COMPETITIVE_POOL = _build_competitive_pool()
_POOLS["competitive_all"] = COMPETITIVE_POOL
POOL_REGISTRY = _POOLS

# Backward-compatible diagnostic name derived from the canonical policy rather
# than maintained as a second hand-edited truth source.
NON_COMPETITIVE_IDS = frozenset(
    question["id"]
    for key in _COMPETITIVE_LEAF_KEYS
    for question in _POOLS[key]
    if question.get("competitive") is not True
)

BATTLE_POOL = COMPETITIVE_POOL

CHALLENGE_POOLS: dict[str, list[dict]] = {
    "easy": [question for question in _POOLS["easy"] if question.get("competitive") is True],
    "medium": [question for question in _POOLS["medium"] if question.get("competitive") is True],
    "hard": [question for question in _POOLS["hard"] if question.get("competitive") is True],
}

_CHALLENGE_DISTRIBUTION = {
    "random20": (("easy", 6), ("medium", 6), ("hard", 8)),
    "hardcore20": (("easy", 4), ("medium", 4), ("hard", 12)),
}


def pick_competitive_challenge_questions(mode: str, *, rng=None) -> list[dict]:
    """Return exactly 20 unique ranking-eligible canonical questions."""
    distribution = _CHALLENGE_DISTRIBUTION.get(mode)
    if distribution is None:
        raise ValueError(f"Неизвестный competitive Challenge mode: {mode!r}")

    source = rng or random
    selected: list[dict] = []
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


def get_pool_by_key(key: str) -> list[dict]:
    """Return a named canonical pool and fail loudly on configuration typos."""
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
    "SOURCE_CATALOG",
    "RANKING_QUARANTINE_IDS",
    "NON_COMPETITIVE_IDS",
    "COMPETITIVE_POOL",
    "BATTLE_POOL",
    "CHALLENGE_POOLS",
    "pick_competitive_challenge_questions",
    "get_pool_by_key",
]
