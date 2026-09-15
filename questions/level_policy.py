"""Where the bank's difficulty ladder actually lives, and what is still missing.

The Chapter-1 pools are the only part of the bank whose *name* states a difficulty
tier: ``easy_p1``/``easy_p2`` are the base tier, ``medium_*`` the core tier,
``hard_*`` the advanced one. Every other pool groups cards by book, chapter or
topic, so for those the audit can only derive a proxy tier from
``claim_type``/``confidence`` - and a proxy is not a review.

This module is the single place that says which is which, so the audit, the report
and the tests cannot drift apart:

* ``POOL_LEVELS`` records the tier a pool name asserts, with its provenance
  spelled out (the name, not a per-card review);
* ``level_for`` returns the recorded level and where it came from;
* ``ladder_summary`` counts the cards on the authored ladder and the cards that
  only have the derived proxy.

Attaching a reviewed level to every card - the honest version of "serve every
level" - is still open for the pools without a tier name; until that review
happens the derived proxy must not be presented as a level.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping

# The tier names used across the bank and the audit report.
LEVELS = ("base", "core", "advanced")

# Pools whose name states a tier, mapped to that tier. Provenance is the pool name
# the chapter-1 authoring used; no card here was individually reviewed for level.
POOL_LEVELS: dict[str, str] = {
    "easy_p1": "base",
    "easy_p2": "base",
    "medium_p1": "core",
    "medium_p2": "core",
    "hard_p1": "advanced",
    "hard_p2": "advanced",
}

SOURCE_POOL = "pool-name"
SOURCE_DERIVED = "derived-from-metadata"


def level_for(pool: str, card: Mapping) -> tuple[str, str]:
    """Return ``(level, source)`` for one card.

    ``source`` is ``SOURCE_POOL`` when the pool name states the tier and
    ``SOURCE_DERIVED`` otherwise, in which case the level is the audit's proxy and
    must be reported as such.
    """
    recorded = POOL_LEVELS.get(pool)
    if recorded:
        return recorded, SOURCE_POOL
    return derived_level(card), SOURCE_DERIVED


def derived_level(card: Mapping) -> str:
    """The proxy tier: application/interpretation are core, greek/history advanced.

    This is the same rule the audit applies to every pool; it is a reading of the
    reviewed metadata, not a second review of the question.
    """
    claim_type = str(card.get("claim_type") or "")
    confidence = str(card.get("confidence") or "")
    if claim_type in {"greek", "history"} or confidence == "contested":
        return "advanced"
    if claim_type in {"interpretation", "application"}:
        return "core"
    return "base"


def ladder_summary(pools: Mapping[str, Iterable[Mapping]]) -> dict[str, object]:
    """Count authored and derived cards per level, for the report and the tests."""
    authored: dict[str, int] = {level: 0 for level in LEVELS}
    derived: dict[str, int] = {level: 0 for level in LEVELS}
    authored_pools: list[str] = []
    for pool, cards in pools.items():
        for card in cards:
            level, source = level_for(pool, card)
            if source == SOURCE_POOL:
                authored[level] += 1
            else:
                derived[level] += 1
        if pool in POOL_LEVELS:
            authored_pools.append(pool)
    return {
        "authored": authored,
        "derived": derived,
        "authored_pools": sorted(authored_pools),
        "authored_cards": sum(authored.values()),
        "derived_cards": sum(derived.values()),
    }


__all__ = [
    "LEVELS",
    "POOL_LEVELS",
    "SOURCE_DERIVED",
    "SOURCE_POOL",
    "derived_level",
    "ladder_summary",
    "level_for",
]
