"""Fail-closed compatibility bridge for residual telegram_controller metadata.

This module owns no product data. It validates the transitional ``bot.py``
metadata against canonical owners, then replaces only the structural values
that the still-large controller can read through its historical ``legacy``
module reference. Presentation copy is preserved byte-for-byte.
"""
from __future__ import annotations

from course_catalog import legacy_level_config
from telegram_conversation_states import (
    ANSWERING,
    BATTLE_ANSWERING,
    CHOOSING_LEVEL,
    validate_legacy_quiz_states,
)

_LEVEL_STRUCTURAL_FIELDS = ("pool_key", "points_per_q", "num_questions")
_LEVEL_PRESENTATION_FIELDS = frozenset({"name"})


def _canonicalized_level_config(legacy_config: object) -> dict[str, dict]:
    """Return a catalog-backed legacy LEVEL_CONFIG view or fail closed.

    The old dictionary contains deployed Russian labels whose wording is not
    product authority. Those labels are intentionally retained. Pool routing,
    scoring weight and question count must match the canonical course catalog
    exactly before the bridge is allowed to replace the mapping.
    """
    if not isinstance(legacy_config, dict):
        raise TypeError("legacy module must expose a LEVEL_CONFIG dict")

    canonical = legacy_level_config()
    bridged: dict[str, dict] = {}
    for key, current in legacy_config.items():
        if key not in canonical:
            raise RuntimeError(f"legacy LEVEL_CONFIG contains unknown course {key!r}")
        if not isinstance(current, dict):
            raise TypeError(f"legacy LEVEL_CONFIG[{key!r}] must be a dict")

        unexpected = set(current) - set(_LEVEL_STRUCTURAL_FIELDS) - _LEVEL_PRESENTATION_FIELDS
        if unexpected:
            raise RuntimeError(
                f"legacy LEVEL_CONFIG[{key!r}] contains unsupported fields: "
                f"{sorted(unexpected)!r}"
            )

        expected = canonical[key]
        for field in _LEVEL_STRUCTURAL_FIELDS:
            if current.get(field) != expected.get(field):
                raise RuntimeError(
                    f"legacy LEVEL_CONFIG[{key!r}].{field} diverged from canonical catalog"
                )

        name = current.get("name")
        if not isinstance(name, str) or not name.strip():
            raise RuntimeError(f"legacy LEVEL_CONFIG[{key!r}].name is invalid")

        bridged[key] = {
            "pool_key": expected["pool_key"],
            "name": name,
            "points_per_q": expected["points_per_q"],
            "num_questions": expected["num_questions"],
        }
    return bridged


def install_legacy_bridge(legacy_module) -> None:
    """Canonicalize quiz states and legacy course-routing metadata atomically."""
    validate_legacy_quiz_states(legacy_module)
    bridged_level_config = _canonicalized_level_config(
        getattr(legacy_module, "LEVEL_CONFIG", None)
    )

    # Mutate only after every validation above succeeds. This keeps startup
    # fail-closed and avoids partial authority replacement on drift.
    legacy_module.CHOOSING_LEVEL = CHOOSING_LEVEL
    legacy_module.ANSWERING = ANSWERING
    legacy_module.BATTLE_ANSWERING = BATTLE_ANSWERING
    legacy_module.LEVEL_CONFIG = bridged_level_config


__all__ = ["install_legacy_bridge"]
