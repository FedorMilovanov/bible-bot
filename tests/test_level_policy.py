"""The difficulty ladder: who states a level, and who only has a proxy.

The bank serves beginner and advanced readers, so "which level is this card?" has
to be answerable from the repository rather than from memory. Today the answer is:
the exposed Chapter-1/TMS, context/linguistics and practical courses carry a human-reviewed per-card
level. Unreviewed pools still use an audit-only proxy from claim_type/confidence.
These tests keep reviewed judgement, legacy pool-name fallback and proxy
provenance distinct.
"""
from __future__ import annotations

import sys
from pathlib import Path

import questions
from questions.level_policy import (
    LEVELS,
    POOL_LEVELS,
    SOURCE_DERIVED,
    SOURCE_POOL,
    SOURCE_REVIEWED,
    derived_level,
    ladder_summary,
    level_for,
)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_question_quality import (  # noqa: E402
    LEAF_POOLS,
    POOL_TIER_CLAIMS,
    _derived_difficulty,
)


def _pools() -> dict[str, list[dict]]:
    return {pool: list(questions.get_pool_by_key(pool)) for pool in LEAF_POOLS}


def test_only_the_tier_named_pools_claim_a_level():
    """The mapping covers exactly the pools whose name states a tier."""
    pools = _pools()
    named = {
        pool
        for pool in pools
        if pool.rsplit("_", 1)[0] in {"easy", "medium", "hard"} and pool.rsplit("_", 1)[1] in {"p1", "p2"}
    }
    assert set(POOL_LEVELS) == named
    assert set(POOL_LEVELS.values()) <= set(LEVELS)


def test_pool_level_comes_with_its_provenance():
    """A pool level is labelled as the pool's, a derived one as derived."""
    card = {"claim_type": "text", "confidence": "high"}
    for pool, level in POOL_LEVELS.items():
        assert level_for(pool, card) == (level, SOURCE_POOL)
    assert level_for("chapter3", card) == ("base", SOURCE_DERIVED)
    assert level_for("chapter3", {"claim_type": "greek"}) == ("advanced", SOURCE_DERIVED)
    assert level_for("chapter3", {"claim_type": "history"}) == ("advanced", SOURCE_DERIVED)
    assert level_for("chapter3", {"claim_type": "text", "confidence": "contested"}) == ("advanced", SOURCE_DERIVED)
    assert level_for("practical_p1", {"claim_type": "application"}) == ("core", SOURCE_DERIVED)


def test_ladder_summary_counts_every_card_once():
    """Authored plus derived cards equal the bank; the ladder pools are listed."""
    summary = ladder_summary(_pools())
    total = sum(len(cards) for cards in _pools().values())
    assert (
        summary["reviewed_cards"]
        + summary["authored_cards"]
        + summary["derived_cards"]
        == total
    )
    assert summary["authored_pools"] == sorted(POOL_LEVELS)
    assert sum(summary["reviewed"].values()) == summary["reviewed_cards"]
    assert sum(summary["authored"].values()) == summary["authored_cards"]
    assert sum(summary["derived"].values()) == summary["derived_cards"]
    assert summary["reviewed_cards"] == 392
    # Every card in the legacy named ladder now has an item review, so pool-name
    # provenance remains only as a compatibility fallback for hypothetical raw cards.
    assert summary["authored_cards"] == 0


def test_audit_and_report_use_the_policy_rule():
    """One rule, one place: the audit delegates to the policy module."""
    for pool in LEAF_POOLS:
        for card in questions.get_pool_by_key(pool):
            expected = card.get("level") or derived_level(card)
            assert _derived_difficulty(card) == expected, card["id"]
    assert POOL_TIER_CLAIMS == {pool.rsplit("_", 1)[0]: level for pool, level in POOL_LEVELS.items()}


def test_reviewed_per_card_levels_have_reviewed_provenance():
    """The first calibrated surface is explicit and does not masquerade as proxy data."""
    reviewed = [
        (pool, card)
        for pool in LEAF_POOLS
        for card in questions.get_pool_by_key(pool)
        if card.get("level")
    ]
    assert len(reviewed) == 392
    for pool, card in reviewed:
        level, source = level_for(pool, card)
        assert level == card["level"]
        assert source == SOURCE_REVIEWED
