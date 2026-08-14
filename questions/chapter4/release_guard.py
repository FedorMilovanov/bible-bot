"""Release guards for Chapter 4 learning-only gameplay and public-card boundaries."""

from __future__ import annotations


def _ids(items) -> set[str]:
    return {str(item.get("id") or "") for item in items}


def validate_gameplay_exclusion(
    chapter4_cards: list[dict],
    *,
    random_all: list[dict],
    competitive_pool: list[dict],
    battle_pool: list[dict],
    challenge_pools: dict[str, list[dict]],
    challenge_fallback: list[dict],
) -> None:
    chapter4_ids = _ids(chapter4_cards)
    surfaces = {
        "random_all": _ids(random_all),
        "competitive": _ids(competitive_pool),
        "battle": _ids(battle_pool),
        "challenge": set().union(*(_ids(pool) for pool in challenge_pools.values())),
        "challenge_fallback": _ids(challenge_fallback),
    }
    for name, ids in surfaces.items():
        overlap = chapter4_ids.intersection(ids)
        if overlap:
            raise ValueError(f"Chapter 4 leaked into {name}: {sorted(overlap)}")


__all__ = ["validate_gameplay_exclusion"]
