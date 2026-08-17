"""Server-authoritative Mini App launch attribution.

Only Telegram-signed initData may feed this module. Client-supplied source or
user identifiers are deliberately outside the API contract.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pymongo import ASCENDING

SOURCE_KEYS = frozenset(
    {
        "site_app",
        "site_home",
        "site_ch1",
        "site_ch2",
        "site_ch3",
        "site_ch4",
        "site_ch5",
        "tg_pin",
        "tg_profile",
        "tg_ch1",
        "tg_ch2",
        "tg_ch3",
        "tg_ch4",
        "tg_ch5",
        "yt_profile",
        "yt_ch1",
        "yt_ch2",
        "yt_ch3",
        "yt_ch4",
        "yt_ch5",
        "vk_pin",
        "vk_ch1",
        "vk_ch2",
        "vk_ch3",
        "vk_ch4",
        "vk_ch5",
    }
)
SAFE_TOKEN = re.compile(r"^[a-z0-9_]{1,48}$")
COMPOSITE_PATTERN = re.compile(r"^v1_([a-z0-9_]{1,48})__([a-z0-9_]{1,48})$")
UNKNOWN_VERSION_PATTERN = re.compile(r"^v[0-9]+_")

COLLECTION_NAME = "miniapp_launch_attributions"
RETENTION_DAYS = 90
TTL_INDEX_NAME = "miniapp_launch_attribution_retention"
SOURCE_TIME_INDEX_NAME = "miniapp_launch_attribution_source_time"

# Return CTAs are intentionally sparse. Every URL here is a reviewed canonical
# public surface. Provider sources and chapter sources without a proven article
# remain absent instead of receiving a guessed or blanket return link.
_RETURN_CONTEXT = {
    "site_app": {
        "kind": "site",
        "label": "Вернуться на сайт",
        "url": "https://gospod-bog.ru/app/",
    },
    "site_home": {
        "kind": "site",
        "label": "Вернуться на сайт",
        "url": "https://gospod-bog.ru/",
    },
    "site_ch3": {
        "kind": "site",
        "label": "Вернуться к статье",
        "url": "https://gospod-bog.ru/hard-texts/duhi-v-temnice-noi-kreshchenie-pobeda/",
    },
    "site_ch4": {
        "kind": "site",
        "label": "Вернуться к статье",
        "url": "https://gospod-bog.ru/hard-texts/blagovestie-mertvym-1-petra-4-5-6/",
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
        return LaunchContext("empty", None, None)

    match = COMPOSITE_PATTERN.fullmatch(raw)
    if match:
        source, destination = match.groups()
        if source not in SOURCE_KEYS or not SAFE_TOKEN.fullmatch(destination):
            return LaunchContext("invalid", None, None)
        return LaunchContext("v1", source, destination)

    # A token presenting an unsupported numbered protocol version must never
    # fall through to the legacy destination path.
    if UNKNOWN_VERSION_PATTERN.match(raw):
        return LaunchContext("invalid", None, None)

    # Legacy launch tokens remain routing-compatible but are never attributed.
    # Unsafe legacy strings are rejected rather than reflected back to the UI.
    if SAFE_TOKEN.fullmatch(raw):
        return LaunchContext("legacy", None, raw)
    return LaunchContext("invalid", None, None)


def _event_key(
    *,
    user_id: int,
    auth_date: int,
    query_id: str | None,
    context: LaunchContext,
) -> str:
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
    if (
        ttl.get("key") != [("retention.expires_at", ASCENDING)]
        or ttl.get("expireAfterSeconds") != 0
    ):
        raise RuntimeError(
            "launch attribution retention index does not match required spec"
        )
    source_time = indexes.get(SOURCE_TIME_INDEX_NAME, {})
    if source_time.get("key") != [
        ("source", ASCENDING),
        ("first_seen_at", ASCENDING),
    ]:
        raise RuntimeError(
            "launch attribution source/time index does not match required spec"
        )


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
