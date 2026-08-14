"""Server-side product policy for canonical question pools.

Question admission and scoring admission are separate decisions. UI surfaces may
describe policy, but only this module decides whether a canonical pool is a
scored/ranked learning surface and how many base points a regular correct answer
is worth.

The policy is intentionally keyed by canonical pool/result keys. Course metadata
resolves to a pool key first and then asks this module for policy; a missing
policy therefore fails closed instead of silently becoming a scored course.
"""
from __future__ import annotations

from dataclasses import dataclass

SCORING_MODE_SCORED = "scored"
SCORING_MODE_LEARNING = "learning"


@dataclass(frozen=True, slots=True)
class PoolPolicy:
    scoring_mode: str
    ranked: bool
    points_per_question: int

    def __post_init__(self) -> None:
        if self.scoring_mode not in {SCORING_MODE_SCORED, SCORING_MODE_LEARNING}:
            raise ValueError(f"invalid scoring mode: {self.scoring_mode!r}")
        if isinstance(self.points_per_question, bool) or self.points_per_question < 0:
            raise ValueError("points_per_question must be a non-negative integer")
        if self.scoring_mode == SCORING_MODE_LEARNING:
            if self.ranked:
                raise ValueError("learning-only pool cannot be ranked")
            if self.points_per_question != 0:
                raise ValueError("learning-only pool must award zero base points")


def _scored(points: int, *, ranked: bool = True) -> PoolPolicy:
    return PoolPolicy(SCORING_MODE_SCORED, ranked, points)


def _learning() -> PoolPolicy:
    return PoolPolicy(SCORING_MODE_LEARNING, False, 0)


# Canonical normal-learning pool/result policies. Competitive question admission
# itself remains in questions.__init__/ranking_policy; this table does not grant
# a question access to Challenge or Battle.
POOL_POLICIES: dict[str, PoolPolicy] = {
    "easy": _scored(1),
    "easy_p1": _scored(1),
    "easy_p2": _scored(1),
    "medium": _scored(2),
    "medium_p1": _scored(2),
    "medium_p2": _scored(2),
    "hard": _scored(3),
    "hard_p1": _scored(3),
    "hard_p2": _scored(3),
    "practical_ch1": _scored(2),
    "practical_p1": _scored(2),
    "practical_p2": _scored(2),
    "linguistics_ch1": _scored(3),
    "linguistics_ch1_2": _scored(3),
    "linguistics_ch1_3": _scored(3),
    "intro1": _scored(2),
    "intro2": _scored(2),
    "intro3": _scored(2),
    "nero": _scored(2),
    "geography": _scored(2),
    "random_all": _scored(1),
    # Chapters 2-5 are product learning modules. Chapters 4/5 are deliberately
    # pre-policy-registered but remain unavailable until their canonical pools
    # are present in questions.POOL_REGISTRY.
    "chapter2": _learning(),
    "chapter3": _learning(),
    "chapter4": _learning(),
    "chapter5": _learning(),
    # Result keys for the existing Challenge persistence path. These do not
    # make random_all or any normal course entry competitive.
    "random20": _scored(1),
    "hardcore20": _scored(2),
}

NON_SCORING_LEARNING_POOLS = frozenset(
    key
    for key, policy in POOL_POLICIES.items()
    if policy.scoring_mode == SCORING_MODE_LEARNING
)


def get_pool_policy(pool_key: str | None) -> PoolPolicy:
    key = str(pool_key or "")
    try:
        return POOL_POLICIES[key]
    except KeyError as exc:
        raise KeyError(f"No product policy registered for pool {key!r}") from exc


def is_non_scoring_learning_pool(pool_key: str | None) -> bool:
    try:
        return get_pool_policy(pool_key).scoring_mode == SCORING_MODE_LEARNING
    except KeyError:
        return False


def points_per_question_for_pool(pool_key: str | None) -> int:
    return get_pool_policy(pool_key).points_per_question


__all__ = [
    "NON_SCORING_LEARNING_POOLS",
    "POOL_POLICIES",
    "PoolPolicy",
    "SCORING_MODE_LEARNING",
    "SCORING_MODE_SCORED",
    "get_pool_policy",
    "is_non_scoring_learning_pool",
    "points_per_question_for_pool",
]
