"""Cross-client policy for canonical question pools.

Content admission and scoring admission are separate decisions. A reviewed pool
may be visible for learning while still being excluded from points, achievements,
leaderboards, Challenge and PvP until a dedicated ranking review is complete.
"""

NON_SCORING_LEARNING_POOLS = frozenset({"chapter2"})


def is_non_scoring_learning_pool(pool_key: str | None) -> bool:
    return str(pool_key or "") in NON_SCORING_LEARNING_POOLS


__all__ = ["NON_SCORING_LEARNING_POOLS", "is_non_scoring_learning_pool"]
