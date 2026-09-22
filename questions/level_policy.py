"""Difficulty provenance for the production question bank.\n\nA difficulty label is trustworthy only when a reviewer judged the cognitive task\nof that specific card. A reviewed level therefore wins over every fallback.\n\nLegacy Chapter-1 pool names still carry a historical level promise and remain a\ncompatibility fallback for unreviewed cards. All other cards may receive an\naudit-only proxy from claim_type/confidence; that proxy is measurement, not a\nproduct-level claim.\n\nlevel_for always returns both the level and its provenance so callers cannot\nsilently confuse reviewed judgement with a heuristic.\n"""
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

SOURCE_REVIEWED = "reviewed-card"
SOURCE_POOL = "pool-name"
SOURCE_DERIVED = "derived-from-metadata"


def level_for(pool: str, card: Mapping) -> tuple[str, str]:
    """Return ``(level, source)`` for one card.

    ``source`` is ``SOURCE_POOL`` when the pool name states the tier and
    ``SOURCE_DERIVED`` otherwise, in which case the level is the audit's proxy and
    must be reported as such.
    """
    reviewed = str(card.get("level") or "").strip()
    if reviewed:
        if reviewed not in LEVELS:
            raise ValueError(f"invalid reviewed level {reviewed!r} for {card.get('id')!r}")
        return reviewed, SOURCE_REVIEWED
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
    reviewed: dict[str, int] = {level: 0 for level in LEVELS}
    authored: dict[str, int] = {level: 0 for level in LEVELS}
    derived: dict[str, int] = {level: 0 for level in LEVELS}
    authored_pools: list[str] = []
    for pool, cards in pools.items():
        for card in cards:
            level, source = level_for(pool, card)
            if source == SOURCE_REVIEWED:
                reviewed[level] += 1
            elif source == SOURCE_POOL:
                authored[level] += 1
            else:
                derived[level] += 1
        if pool in POOL_LEVELS:
            authored_pools.append(pool)
    return {
        "reviewed": reviewed,
        "authored": authored,
        "derived": derived,
        "authored_pools": sorted(authored_pools),
        "reviewed_cards": sum(reviewed.values()),
        "authored_cards": sum(authored.values()),
        "derived_cards": sum(derived.values()),
    }


__all__ = [
    "LEVELS",
    "POOL_LEVELS",
    "SOURCE_DERIVED",
    "SOURCE_POOL",
    "SOURCE_REVIEWED",
    "derived_level",
    "ladder_summary",
    "level_for",
]
