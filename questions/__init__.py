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
from .chapter2.reviewed import CHAPTER2_REVIEWED_QUESTIONS
from .chapter3.challenge_taxonomy import CHAPTER3_CHALLENGE_TAXONOMY
from .chapter3.ranking_authority import CHAPTER3_RANKING_AUTHORIZED_IDS
from .chapter3.reviewed import CHAPTER3_REVIEWED_QUESTIONS
from .chapter5.reviewed import CHAPTER5_REVIEWED_QUESTIONS
from .content_truth import RANKING_QUARANTINE_IDS, curate_pool
from .content_truth_review import apply_review_overrides
from .intro import (
    intro_part1_questions as _raw_intro1,
    intro_part2_questions as _raw_intro2,
    intro_part3_questions as _raw_intro3,
)
from .ranking_policy import SOURCE_REVIEWED_RANKING_IDS, ranking_eligible
from .source_registry import SOURCE_CATALOG


def _canonical(raw: list[dict], key: str) -> list[dict]:
    return apply_review_overrides(curate_pool(raw, pool_key=key))


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

# Chapters 2, 3 and 5 cross the product boundary only through reviewed aggregates.
# Their normal-learning pools remain non-scoring through questions.pool_policy.
chapter2_questions = list(CHAPTER2_REVIEWED_QUESTIONS)
chapter3_questions = list(CHAPTER3_REVIEWED_QUESTIONS)
chapter5_questions = list(CHAPTER5_REVIEWED_QUESTIONS)


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
    "chapter2": chapter2_questions,
    "chapter3": chapter3_questions,
    "chapter5": chapter5_questions,
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

# random_all remains the legacy Chapter-1/context learning pool. Chapters 2, 3
# and 5 are admitted separately so normal-learning exposure cannot silently
# enlarge Challenge 20 or other legacy random behavior.
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


def _build_chapter1_competitive_pool() -> list[dict]:
    return [
        question
        for question in _dedupe_pool(_COMPETITIVE_LEAF_KEYS)
        if ranking_eligible(question)
    ]


def _build_chapter3_authorized_competitive_pool() -> list[dict]:
    """Resolve the explicit Chapter-3 authority against reviewed product cards."""
    result = [
        question
        for question in chapter3_questions
        if str(question.get("id") or "").strip() in CHAPTER3_RANKING_AUTHORIZED_IDS
    ]
    resolved_ids = {str(question.get("id") or "").strip() for question in result}
    if resolved_ids != set(CHAPTER3_RANKING_AUTHORIZED_IDS):
        missing = sorted(set(CHAPTER3_RANKING_AUTHORIZED_IDS) - resolved_ids)
        extra = sorted(resolved_ids - set(CHAPTER3_RANKING_AUTHORIZED_IDS))
        raise ValueError(
            "Chapter 3 ranking authority does not resolve exactly against reviewed bank: "
            f"missing={missing}, extra={extra}"
        )
    if any(not ranking_eligible(question) for question in result):
        invalid = [question["id"] for question in result if not ranking_eligible(question)]
        raise ValueError(f"Authorized Chapter 3 cards fail structural ranking policy: {invalid}")
    return result


CHAPTER1_COMPETITIVE_POOL = _build_chapter1_competitive_pool()
CHAPTER3_AUTHORIZED_COMPETITIVE_POOL = _build_chapter3_authorized_competitive_pool()
COMPETITIVE_POOL = CHAPTER1_COMPETITIVE_POOL + CHAPTER3_AUTHORIZED_COMPETITIVE_POOL
_POOLS["competitive_all"] = COMPETITIVE_POOL
POOL_REGISTRY = _POOLS

NON_COMPETITIVE_IDS = frozenset(
    question["id"]
    for key in _COMPETITIVE_LEAF_KEYS
    for question in _POOLS[key]
    if not ranking_eligible(question)
)

# Legacy PvP imports BATTLE_POOL directly. Only the explicitly authorized twelve
# Chapter-3 cards join this surface; Chapter 5 remains outside gameplay.
BATTLE_POOL = COMPETITIVE_POOL


def _resolve_chapter3_challenge_taxonomy() -> dict[str, list[dict]]:
    """Resolve reviewed taxonomy IDs against the exact authorized Chapter-3 pool."""
    authorized_by_id = {
        str(question.get("id") or "").strip(): question
        for question in CHAPTER3_AUTHORIZED_COMPETITIVE_POOL
    }
    resolved: dict[str, list[dict]] = {}
    seen: set[str] = set()
    for level in ("easy", "medium", "hard"):
        level_ids = tuple(CHAPTER3_CHALLENGE_TAXONOMY[level])
        missing = [qid for qid in level_ids if qid not in authorized_by_id]
        if missing:
            raise ValueError(
                f"Chapter 3 Challenge taxonomy contains unresolved {level} ids: {missing}"
            )
        if seen.intersection(level_ids):
            overlap = sorted(seen.intersection(level_ids))
            raise ValueError(f"Chapter 3 Challenge taxonomy overlaps across levels: {overlap}")
        seen.update(level_ids)
        resolved[level] = [authorized_by_id[qid] for qid in level_ids]

    if seen != set(CHAPTER3_RANKING_AUTHORIZED_IDS):
        missing = sorted(set(CHAPTER3_RANKING_AUTHORIZED_IDS) - seen)
        extra = sorted(seen - set(CHAPTER3_RANKING_AUTHORIZED_IDS))
        raise ValueError(
            "Chapter 3 Challenge taxonomy must cover ranking authority exactly: "
            f"missing={missing}, extra={extra}"
        )
    return resolved


CHAPTER3_CHALLENGE_POOLS = _resolve_chapter3_challenge_taxonomy()

# Challenge keeps its established category quotas. Chapter 3 enters only through
# the reviewed taxonomy: 6 easy + 6 medium + 0 hard. Hard remains Chapter-1-only.
CHALLENGE_POOLS: dict[str, list[dict]] = {
    "easy": [question for question in _POOLS["easy"] if ranking_eligible(question)]
    + CHAPTER3_CHALLENGE_POOLS["easy"],
    "medium": [question for question in _POOLS["medium"] if ranking_eligible(question)]
    + CHAPTER3_CHALLENGE_POOLS["medium"],
    "hard": [question for question in _POOLS["hard"] if ranking_eligible(question)]
    + CHAPTER3_CHALLENGE_POOLS["hard"],
}

# Fallback stays Chapter-1-only even after explicit Chapter-3 taxonomy admission.
# Chapter 3 can enter Challenge only by its reviewed difficulty bucket, never by
# a generic shortage fallback from the general competitive pool.
CHALLENGE_FALLBACK_POOL = CHAPTER1_COMPETITIVE_POOL

_CHALLENGE_DISTRIBUTION = {
    "random20": (("easy", 6), ("medium", 6), ("hard", 8)),
    "hardcore20": (("easy", 4), ("medium", 4), ("hard", 12)),
}


def pick_competitive_challenge_questions(mode: str, *, rng=None) -> list[dict]:
    """Return exactly 20 unique ranking-authorized questions by reviewed taxonomy."""
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
            for question in CHALLENGE_FALLBACK_POOL
            if str(question.get("id") or "").strip() not in seen
        ]
        needed = 20 - len(selected)
        if len(remainder) < needed:
            raise ValueError(
                "Challenge-authorized question bank contains fewer than 20 unique questions"
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
    "chapter2_questions",
    "chapter3_questions",
    "chapter5_questions",
    "POOL_REGISTRY",
    "SOURCE_CATALOG",
    "RANKING_QUARANTINE_IDS",
    "SOURCE_REVIEWED_RANKING_IDS",
    "NON_COMPETITIVE_IDS",
    "CHAPTER1_COMPETITIVE_POOL",
    "CHAPTER3_AUTHORIZED_COMPETITIVE_POOL",
    "COMPETITIVE_POOL",
    "BATTLE_POOL",
    "CHAPTER3_CHALLENGE_POOLS",
    "CHALLENGE_POOLS",
    "CHALLENGE_FALLBACK_POOL",
    "pick_competitive_challenge_questions",
    "get_pool_by_key",
]
