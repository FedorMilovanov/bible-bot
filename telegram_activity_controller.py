"""Non-blocking activity touch for lightweight production presentation callbacks."""
from __future__ import annotations

import asyncio
import time


def _touch_memory(legacy_module, user_id: int) -> None:
    """Preserve the legacy in-memory activity timestamp without doing I/O."""
    user_data = getattr(legacy_module, "user_data", None)
    if not isinstance(user_data, dict):
        return
    data = user_data.get(user_id)
    if isinstance(data, dict):
        data["last_activity"] = time.time()


async def touch_presentation(update, *, legacy_module):
    """Touch callback activity while keeping synchronous Mongo work off PTB."""
    query = update.callback_query
    user_id = query.from_user.id
    _touch_memory(legacy_module, user_id)
    await asyncio.to_thread(legacy_module.touch_user_activity, user_id)
    return query
