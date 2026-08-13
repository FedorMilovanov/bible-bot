"""Validation for persisted non-scoring retry-error practice attempts."""
from __future__ import annotations


class LegacyRetryPolicyInvalid(RuntimeError):
    """Persisted retry policy is contradictory or malformed."""


def persisted_is_retry(session: dict) -> bool:
    """Return the durable retry flag, rejecting ambiguous/corrupt evidence."""
    if not isinstance(session, dict):
        raise LegacyRetryPolicyInvalid("quiz session must be a dict")
    value = session.get("is_retry", False)
    if not isinstance(value, bool):
        raise LegacyRetryPolicyInvalid("persisted is_retry must be a boolean")
    if value and session.get("mode") != "level":
        raise LegacyRetryPolicyInvalid("retry-error practice must use level mode")
    return value
