"""Non-blocking activity touch for lightweight production presentation callbacks."""
from __future__ import annotations

import asyncio
import time

from database import touch_user_activity


def _touch_memory(user_data, user_id: int) -> None:
    """Preserve the process-local activity timestamp without doing I/O."""
    if not isinstance(user_data, dict):
        return
    data = user_data.get(user_id)
    if isinstance(data, dict):
        data["last_activity"] = time.time()


async def touch_presentation(update, *, user_data):
    """Touch callback activity while keeping synchronous Mongo work off PTB."""
    query = update.callback_query
    user_id = query.from_user.id
    _touch_memory(user_data, user_id)
    await asyncio.to_thread(touch_user_activity, user_id)
    return query
