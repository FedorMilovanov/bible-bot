"""Server-authoritative Mini App launch attribution.

Only Telegram-signed initData may feed this module. Client-supplied source or
user identifiers are deliberately outside the API contract.
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pymongo import ASCENDING

logger = logging.getLogger(__name__)

SOURCE_KEYS = frozenset(
    {
        "site_app",
        "site_ch2",
        "site_library",
        "tg_pin",
        "tg_menu",
        "yt_desc",
        "yt_comment",
        "vk_post",
        "vk_menu",
    }
)
DESTINATION_PATTERN = re.compile(r"^(home|chapter[1-5]|level_[a-z0-9_]{1,40})$")
COMPOSITE_PATTERN = re.compile(r"^v1_([a-z0-9_]{2,32})__([a-z0-9_]{1,48})$")

COLLECTION_NAME = "miniapp_launch_attributions"
RETENTION_DAYS = 90
TTL_INDEX_NAME = "miniapp_launch_attribution_retention"
SOURCE_TIME_INDEX_NAME = "miniapp_launch_attribution_source_time"

_RETURN_CONTEXT = {
    "site_app": {
        "kind": "site",
        "label": "Вернуться на сайт",
        "url": "https://gospod-bog.ru/app/",
    },
    "site_library": {
        "kind": "library",
        "label": "Вернуться в библиотеку",
        "url": "https://gospod-bog.ru/",
    },
    "site_ch2": {
        "kind": "library",
        "label": "Вернуться к чтению",
        "url": "https://gospod-bog.ru/",
    },
}


@dataclass(frozen=True)
class LaunchContext:
    kind: str
    source: str | None
    destination: str | None

    def public_dict(self) -> dict:
        result = {
            "kind": self.kind,
            "source": self.source,
            "destination": self.destination,
            "return_context": None,
        }
        if self.kind == "v1" and self.source in _RETURN_CONTEXT:
            result["return_context"] = dict(_RETURN_CONTEXT[self.source])
        return result


def parse_launch_param(value: str | None) -> LaunchContext:
    """Parse the canonical launch contract without trusting browser state."""
    raw = (value or "").strip()
    if not raw:
        return LaunchContext("none", None, None)

    match = COMPOSITE_PATTERN.fullmatch(raw)
    if match:
        source, destination = match.groups()
        if source not in SOURCE_KEYS or not DESTINATION_PATTERN.fullmatch(destination):
            return LaunchContext("invalid", None, None)
        return LaunchContext("v1", source, destination)

    # Any token that presents itself as versioned must fail closed. It must not
    # fall through to legacy routing, including unknown future versions.
    if raw.startswith("v"):
        return LaunchContext("invalid", None, None)

    if DESTINATION_PATTERN.fullmatch(raw):
        return LaunchContext("legacy", None, raw)
    return LaunchContext("invalid", None, None)


def _event_key(*, user_id: int, auth_date: int, query_id: str | None, context: LaunchContext) -> str:
    """Return a stable retry/replay key without persisting Telegram identity."""
    material = "\x00".join(
        (
            str(user_id),
            str(auth_date),
            query_id or "",
            context.source or "",
            context.destination or "",
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def ensure_launch_attribution_indexes(database) -> None:
    """Create and verify bounded-retention/query indexes for attribution."""
    collection = database[COLLECTION_NAME]
    collection.create_index(
        [("retention.expires_at", ASCENDING)],
        name=TTL_INDEX_NAME,
        expireAfterSeconds=0,
    )
    collection.create_index(
        [("source", ASCENDING), ("first_seen_at", ASCENDING)],
        name=SOURCE_TIME_INDEX_NAME,
    )

    indexes = collection.index_information()
    ttl = indexes.get(TTL_INDEX_NAME, {})
    if ttl.get("key") != [("retention.expires_at", ASCENDING)] or ttl.get("expireAfterSeconds") != 0:
        raise RuntimeError("launch attribution retention index does not match required spec")
    source_time = indexes.get(SOURCE_TIME_INDEX_NAME, {})
    if source_time.get("key") != [("source", ASCENDING), ("first_seen_at", ASCENDING)]:
        raise RuntimeError("launch attribution source/time index does not match required spec")


def persist_launch_attribution(
    *,
    database,
    user_id: int,
    auth_date: int,
    query_id: str | None,
    context: LaunchContext,
    now: datetime | None = None,
) -> bool:
    """Idempotently persist one signed v1 launch; return whether it is represented.

    Repeated bootstrap calls use the same deterministic ``_id`` and therefore
    cannot increment attribution counts. The 90-day expiry is anchored to the
    signed Telegram auth timestamp and is never extended by retries.
    """
    if context.kind != "v1" or not context.source or not context.destination:
        return False

    ensure_launch_attribution_indexes(database)
    seen_at = now or datetime.now(UTC)
    auth_seen_at = datetime.fromtimestamp(auth_date, UTC)
    event_key = _event_key(
        user_id=user_id,
        auth_date=auth_date,
        query_id=query_id,
        context=context,
    )
    document = {
        "_id": event_key,
        "version": "v1",
        "source": context.source,
        "destination": context.destination,
        "telegram_auth_date": auth_seen_at,
        "first_seen_at": seen_at,
        "retention": {
            "expires_at": auth_seen_at + timedelta(days=RETENTION_DAYS),
        },
    }
    collection = database[COLLECTION_NAME]
    collection.update_one(
        {"_id": event_key},
        {"$setOnInsert": document},
        upsert=True,
    )
    return True
