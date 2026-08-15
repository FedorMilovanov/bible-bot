"""Production Mini App quiz-start path with server-authoritative course policy."""
from __future__ import annotations

import logging
import random
import time
import uuid

from pymongo.errors import DuplicateKeyError, PyMongoError

import questions
from course_catalog import (
    SURFACE_MINIAPP,
    CourseCatalogError,
    CourseUnavailableError,
    course_for_pool,
    resolve_course,
    resolve_course_pool,
)
from . import quiz as core
from .db_hardening import OPEN_STATUSES

logger = logging.getLogger(__name__)
_CLIENT_POLICY_FIELDS = frozenset(
    {"ranked", "scoring_mode", "points_per_question", "score_multiplier"}
)


def _resolve_normal_course(payload: dict, mode: str):
    """Resolve new course_key or a safe legacy pool_key through the catalog."""
    forbidden = sorted(_CLIENT_POLICY_FIELDS.intersection(payload))
    if forbidden:
        raise CourseCatalogError(
            f"client cannot override server course policy: {', '.join(forbidden)}"
        )

    course_key = str(payload.get("course_key", "")).strip()
    legacy_pool_key = str(payload.get("pool_key", "")).strip()
    if course_key:
        entry = resolve_course(course_key, surface=SURFACE_MINIAPP, mode=mode)
        if legacy_pool_key and legacy_pool_key != entry.pool_key:
            raise CourseCatalogError("course_key and pool_key do not match")
        return entry

    # Backwards compatibility for a previously deployed Mini App bundle.  A
    # raw pool is accepted only when it maps unambiguously to one currently
    # exposed Mini App course; it never bypasses catalog availability/policy.
    if legacy_pool_key:
        entry = course_for_pool(legacy_pool_key, surface=SURFACE_MINIAPP)
        if entry is None:
            raise CourseCatalogError("pool is not an exposed Mini App course")
        return resolve_course(entry.key, surface=SURFACE_MINIAPP, mode=mode)

    raise CourseCatalogError("course_key is required")


def start_quiz(user: dict, payload: dict) -> tuple[dict | None, str | None, int]:
    """Create/resume one durable Mini App quiz without mixing ranking pools."""
    mode = str(payload.get("mode", "relaxed")).strip()
    if mode not in core.MODE_CONFIG:
        return None, "invalid quiz mode", 400

    is_challenge = bool(payload.get("challenge"))
    course_key: str | None = None
    if is_challenge:
        # Challenge remains an independent competitive authority.  It is not a
        # course-catalog entry and cannot be pointed at Chapter 2/3/etc.
        pool_key = str(payload.get("pool_key", "")).strip()
        if pool_key != "random_all":
            return None, "challenge requires random_all pool", 400
        if str(payload.get("course_key", "")).strip():
            return None, "challenge does not accept course_key", 400
        if _CLIENT_POLICY_FIELDS.intersection(payload):
            return None, "client cannot override server course policy", 400
        if mode not in {"relaxed", "speed"}:
            return None, "invalid challenge mode", 400
        expected_count = 20
        pool = None
    else:
        try:
            entry = _resolve_normal_course(payload, mode)
        except CourseUnavailableError:
            return None, "course unavailable", 409
        except CourseCatalogError:
            return None, "invalid course selection", 400
        course_key = entry.key
        pool_key = entry.pool_key
        expected_count = entry.default_question_count
        try:
            pool = resolve_course_pool(entry)
        except CourseUnavailableError:
            return None, "course unavailable", 409
        except KeyError:
            return None, "question pool unavailable", 409
        except Exception:
            logger.exception("failed to load course question pool")
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
                if course_key:
                    resumed["course_key"] = course_key
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
        "course_key": course_key,
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
    if course_key:
        current["course_key"] = course_key
    return current, None, 200