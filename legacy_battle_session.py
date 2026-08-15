"""Strict creation/discovery/join/access policy for durable PvP question progress."""
from __future__ import annotations

from datetime import timedelta

from pymongo import DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

from legacy_battle_callback_protocol import callback_matches_battle
from legacy_battle_progress import battle_question_id
from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE
from legacy_battle_ready_delivery import battle_ready_delivery_marker


class LegacyBattleSessionUnavailable(RuntimeError):
    """Raised when durable PvP session storage cannot reach MongoDB."""


class LegacyBattleSessionConflict(RuntimeError):
    """Raised when a durable PvP create/join/access request loses a state race."""


def _database():
    import database

    return database


def _battle_collection():
    collection = getattr(_database(), "battles_collection", None)
    if collection is None:
        raise LegacyBattleSessionUnavailable("battle collection is unavailable")
    return collection


def _required_battle_id(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("battle_id is required")
    return value.strip()


def _required_user_id(value, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _required_name(value, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    return value.strip()


def _required_positive_int(value, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _validated_questions(value) -> list[dict]:
    if not isinstance(value, list) or not value:
        raise ValueError("battle questions are required")
    questions = []
    for question in value:
        battle_question_id(question)
        questions.append(question)
    return questions


def _open_participant_filter(user_id: int) -> dict:
    return {
        "$or": [{"creator_id": user_id}, {"opponent_id": user_id}],
        "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
        "status": {"$in": ["waiting", "in_progress"]},
        "final_claimed": {"$ne": True},
    }


def create_durable_battle(
    *,
    battle_id: str,
    creator_id: int,
    creator_name: str,
    questions: list[dict],
) -> dict:
    """Insert one new battle already bound to the durable-progress protocol."""
    battle_id = _required_battle_id(battle_id)
    creator_id = _required_user_id(creator_id, "creator_id")
    creator_name = _required_name(creator_name, "creator_name")
    questions = _validated_questions(questions)
    database = _database()
    collection = _battle_collection()
    now = database._now_utc()
    doc = {
        "_id": battle_id,
        "creator_id": creator_id,
        "creator_name": creator_name,
        "questions": questions,
        "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
        "status": "waiting",
        "creator_score": 0,
        "creator_answers": [],
        "creator_time": 0,
        "creator_points": 0,
        "creator_finished": False,
        "opponent_id": None,
        "opponent_name": None,
        "opponent_score": 0,
        "opponent_answers": [],
        "opponent_time": 0,
        "opponent_points": 0,
        "opponent_finished": False,
        "created_at": now.isoformat(),
        "created_at_dt": now,
        "updated_at": now.isoformat(),
    }
    try:
        collection.insert_one(doc)
    except DuplicateKeyError as exc:
        raise LegacyBattleSessionConflict("battle id already exists") from exc
    except PyMongoError as exc:
        raise LegacyBattleSessionUnavailable("battle creation failed") from exc
    return doc


def get_waiting_durable_battles(*, limit: int = 10, max_age_minutes: int = 10) -> list[dict]:
    """List only waiting battles that were created under durable-progress v1."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0 or limit > 100:
        raise ValueError("limit must be an integer between 1 and 100")
    max_age_minutes = _required_positive_int(max_age_minutes, "max_age_minutes")
    database = _database()
    collection = _battle_collection()
    cutoff = database._now_utc() - timedelta(minutes=max_age_minutes)
    try:
        cursor = collection.find(
            {
                "status": "waiting",
                "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
                "created_at_dt": {"$gte": cutoff},
                "final_claimed": {"$ne": True},
            }
        ).sort("created_at_dt", DESCENDING).limit(limit)
        return list(cursor)
    except PyMongoError as exc:
        raise LegacyBattleSessionUnavailable("waiting battle lookup failed") from exc


def get_owned_open_durable_battle(battle_id: str, user_id: int) -> dict | None:
    """Load one open durable battle only when the caller is a persisted participant."""
    battle_id = _required_battle_id(battle_id)
    user_id = _required_user_id(user_id, "user_id")
    collection = _battle_collection()
    try:
        return collection.find_one({"_id": battle_id, **_open_participant_filter(user_id)})
    except PyMongoError as exc:
        raise LegacyBattleSessionUnavailable("owned battle lookup failed") from exc


def resolve_owned_open_battle_callback(user_id: int, callback_token: str) -> dict:
    """Resolve one semantic callback token to exactly one open owned battle.

    Telegram callbacks carry only a compact battle fingerprint. After a process
    restart there is deliberately no RAM battle id to trust, so the durable
    participant set is queried first and the fingerprint is matched in-process.
    Ambiguous/corrupt matches fail closed instead of picking an arbitrary battle.
    """
    user_id = _required_user_id(user_id, "user_id")
    if not isinstance(callback_token, str) or not callback_token:
        raise ValueError("callback_token is required")
    collection = _battle_collection()
    try:
        candidates = list(collection.find(_open_participant_filter(user_id)).limit(100))
    except PyMongoError as exc:
        raise LegacyBattleSessionUnavailable("battle callback lookup failed") from exc
    matches = [
        battle
        for battle in candidates
        if isinstance(battle, dict)
        and isinstance(battle.get("_id"), str)
        and callback_matches_battle(battle["_id"], callback_token)
    ]
    if len(matches) != 1:
        raise LegacyBattleSessionConflict("battle callback is stale or ambiguous")
    return matches[0]


def claim_durable_battle_opponent(
    battle_id: str,
    user_id: int,
    user_name: str,
    *,
    max_age_minutes: int = 10,
) -> dict | None:
    """Atomically claim a non-expired opponent slot on durable-progress v1."""
    battle_id = _required_battle_id(battle_id)
    user_id = _required_user_id(user_id, "user_id")
    user_name = _required_name(user_name, "user_name")
    max_age_minutes = _required_positive_int(max_age_minutes, "max_age_minutes")
    database = _database()
    collection = _battle_collection()
    now = database._now_utc()
    cutoff = now - timedelta(minutes=max_age_minutes)
    try:
        return collection.find_one_and_update(
            {
                "_id": battle_id,
                "status": "waiting",
                "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
                "created_at_dt": {"$gte": cutoff},
                "opponent_id": None,
                "creator_id": {"$ne": user_id},
                "final_claimed": {"$ne": True},
            },
            {
                "$set": {
                    "opponent_id": user_id,
                    "opponent_name": user_name,
                    "status": "in_progress",
                    "joined_at_dt": now,
                    "updated_at": now.isoformat(),
                    "creator_ready_delivery": battle_ready_delivery_marker(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
    except PyMongoError as exc:
        raise LegacyBattleSessionUnavailable("battle opponent claim failed") from exc