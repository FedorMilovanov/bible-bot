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


_LEARNING_ALL_LEAF_KEYS = [
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
_POOLS["learning_all"] = _dedupe_pool(_LEARNING_ALL_LEAF_KEYS)


NON_COMPETITIVE_IDS = frozenset(
    {
        "easy_02",
        "easy_03",
        "easy_04",
        "easy_12",
        "med_01",
        "med_02",
        "med_03",
        "med_13",
        "med_15",
        "hard_02",
        "hard_03",
        "hard_11",
        "hard_12",
        "hard_13",
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
# web_api.quiz reserves random_all for Challenge 20; keep the compatibility key
# authoritative by pointing it at exactly the same eligible bank.
_POOLS["random_all"] = COMPETITIVE_POOL
POOL_REGISTRY = _POOLS

BATTLE_POOL = COMPETITIVE_POOL

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
