"""Process-local production maintenance jobs with no durable-state authority."""
from __future__ import annotations

import logging
import time
from collections.abc import MutableMapping

from config import GC_INTERVAL as GC_INTERVAL
from config import GC_STALE_THRESHOLD
from telegram_quiz_runtime_state import get_user_data

logger = logging.getLogger(__name__)


def cleanup_stale_userdata(
    user_data: MutableMapping,
    *,
    stale_threshold: float = GC_STALE_THRESHOLD,
    now: float | None = None,
) -> int:
    """Drop stale process-local session mirrors and return the deletion count."""
    current = time.time() if now is None else now
    stale = [
        user_id
        for user_id, data in list(user_data.items())
        if current - data.get("last_activity", current) > stale_threshold
    ]
    for user_id in stale:
        user_data.pop(user_id, None)
    return len(stale)


async def cleanup_stale_userdata_job(
    context,
    *,
    stale_threshold: float = GC_STALE_THRESHOLD,
) -> None:
    """PTB JobQueue adapter for process-local garbage collection."""
    del context
    deleted = cleanup_stale_userdata(
        get_user_data(),
        stale_threshold=stale_threshold,
    )
    if deleted:
        logger.info("GC removed %d stale user_data entries", deleted)
