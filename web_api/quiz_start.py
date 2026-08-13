"""Production Mini App quiz-start path with explicit competitive selection."""
from __future__ import annotations

import logging
import random
import time
import uuid

from pymongo.errors import DuplicateKeyError, PyMongoError

import questions
from . import quiz as core
from .db_hardening import OPEN_STATUSES

logger = logging.getLogger(__name__)


def start_quiz(user: dict, payload: dict) -> tuple[dict | None, str | None, int]:
    """Create/resume one durable Mini App quiz without mixing ranking pools."""
    pool_key = str(payload.get("pool_key", "")).strip()
    mode = str(payload.get("mode", "relaxed")).strip()
    if mode not in core.MODE_CONFIG:
        return None, "invalid quiz mode", 400

    is_challenge = bool(payload.get("challenge"))
    if is_challenge:
        if pool_key != "random_all":
            return None, "challenge requires random_all pool", 400
        if mode not in {"relaxed", "speed"}:
            return None, "invalid challenge mode", 400
        expected_count = 20
        pool = None
    else:
        if pool_key == "random_all":
            return None, "random_all is reserved for Challenge 20", 400
        expected_count = 10
        try:
            pool = questions.get_pool_by_key(pool_key)
        except KeyError:
            return None, "unknown question pool", 404
        except Exception:
            logger.exception("failed to load question pool")
            return None, "question pool unavailable", 503
        if len(pool) < expected_count:
            return None, f"question pool contains fewer than {expected_count} questions", 409

    if "count" in payload:
        try:
            requested_count = int(payload["count"])
        except (TypeError, ValueError):
            return None, "invalid question count", 400
        if requested_count != expected_count:
            return None, f"question count must be {expected_count}", 400
    count = expected_count

    sessions = core.miniapp_sessions()
    if sessions is None:
        return None, "database unavailable", 503

    user_id = str(user["id"])
    try:
        open_session = sessions.find_one(
            {"user_id": user_id, "status": {"$in": list(OPEN_STATUSES)}}
        )
    except PyMongoError:
        logger.exception("failed to resolve open Mini App session")
        return None, "database temporarily unavailable", 503
    except Exception:
        logger.exception("unexpected open Mini App session lookup failure")
        return None, "could not resolve open quiz session", 500

    if open_session:
        if open_session.get("status") == "in_progress":
            resumed = core._matching_active_start_payload(
                open_session,
                pool_key=pool_key,
                mode=mode,
                is_challenge=is_challenge,
                count=count,
            )
            if resumed is not None:
                return resumed, None, 200

        open_total = int(
            open_session.get("question_count")
            or len(open_session.get("questions") or [])
        )
        open_index = int(open_session.get("current_index", 0))
        if open_total <= 0 or open_index != open_total:
            if open_session.get("status") == "in_progress":
                return None, "another active quiz is in progress; finish it before starting another", 409
            return None, "unfinished quiz result state is inconsistent", 409

        finalized = core._finalize_quiz(open_session, user)
        if finalized is None:
            return None, "previous quiz result finalization is incomplete", 503

    try:
        from database import get_user_stats, init_user_stats

        init_user_stats(
            int(user["id"]),
            user.get("username", ""),
            user.get("first_name", ""),
        )
        if get_user_stats(int(user["id"])) is None:
            return None, "user profile unavailable", 503
    except Exception:
        logger.exception("failed to initialise user profile")
        return None, "user profile unavailable", 503

    try:
        if is_challenge:
            challenge_mode = "hardcore20" if mode == "speed" else "random20"
            selected = questions.pick_competitive_challenge_questions(challenge_mode)
        else:
            selected = random.sample(pool, count)
    except ValueError:
        logger.exception("question selection failed")
        return None, "question pool unavailable", 503

    if len(selected) != count:
        logger.error(
            "question selection returned %d items instead of %d",
            len(selected),
            count,
        )
        return None, "question selection returned an invalid count", 503

    try:
        prepared = [core.prepare_question(question) for question in selected]
    except (TypeError, ValueError):
        logger.exception("invalid question data selected for quiz start")
        return None, "question data is invalid", 500

    cfg = dict(core.MODE_CONFIG[mode])
    if is_challenge and mode == "speed":
        cfg["time_limit"] = 10

    now = core._now()
    session_id = str(uuid.uuid4())
    document = {
        "_id": session_id,
        "user_id": user_id,
        "username": user.get("username", ""),
        "first_name": user.get("first_name", ""),
        "status": "in_progress",
        "pool_key": pool_key,
        "stats_level_key": core.stats_level_key(
            pool_key,
            is_challenge=is_challenge,
            mode=mode,
        ),
        "mode": mode,
        "is_challenge": is_challenge,
        "questions": prepared,
        "question_count": len(prepared),
        "current_index": 0,
        "correct_count": 0,
        "current_streak": 0,
        "max_streak": 0,
        "answered": [],
        "time_limit": cfg["time_limit"],
        "score_multiplier": cfg["multiplier"],
        "started_at_dt": now,
        "completed_at_dt": None,
        "completion_time_protocol": core.COMPLETION_TIME_PROTOCOL_DURABLE,
        "updated_at_dt": now,
        "question_sent_at": time.time(),
        "leaderboard_recorded": False,
    }
    try:
        sessions.insert_one(document)
    except DuplicateKeyError:
        logger.info("open Mini App session already exists for user %s", user["id"])
        return None, "another unfinished quiz already exists; retry start", 409
    except PyMongoError:
        logger.exception("database unavailable while creating Mini App quiz session")
        return None, "database temporarily unavailable", 503
    except Exception:
        logger.exception("unexpected failure while creating Mini App quiz session")
        return None, "could not create quiz session", 500

    current = core._active_session_payload(document, resumed=False)
    if current is None:
        return None, "created quiz session is inconsistent", 500
    return current, None, 200
