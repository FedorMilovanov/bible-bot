"""Narrow adapter around the existing process-local report draft mapping.

The durable report controller still owns report acceptance and delivery. This
module only prevents the production composition root from reaching directly
into the transitional legacy mapping while preserving that exact single mapping.
"""
from __future__ import annotations

import bot as legacy


def drop_report_draft(user_id: int) -> bool:
    """Drop one process-local report draft; return whether one existed."""
    return legacy.report_drafts.pop(user_id, None) is not None
