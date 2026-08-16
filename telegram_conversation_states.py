"""Canonical python-telegram-bot conversation-state constants for quiz flows."""
from __future__ import annotations


CHOOSING_LEVEL, ANSWERING, BATTLE_ANSWERING = range(3)


def validate_legacy_quiz_states(legacy_module) -> None:
    """Fail closed unless transitional legacy quiz-state values match exactly."""
    expected = {
        "CHOOSING_LEVEL": CHOOSING_LEVEL,
        "ANSWERING": ANSWERING,
        "BATTLE_ANSWERING": BATTLE_ANSWERING,
    }
    actual = {name: getattr(legacy_module, name, None) for name in expected}
    if actual != expected:
        raise RuntimeError(
            "Legacy quiz conversation states diverged from canonical values: "
            f"expected={expected!r}, actual={actual!r}"
        )


__all__ = [
    "ANSWERING",
    "BATTLE_ANSWERING",
    "CHOOSING_LEVEL",
    "validate_legacy_quiz_states",
]
