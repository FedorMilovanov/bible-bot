"""Flask application for health, Mini App static files and API routes."""
from __future__ import annotations

import logging
import os
import pathlib
import random
from datetime import UTC, datetime

from flask import Flask, jsonify, request, send_file, send_from_directory

from course_catalog import COURSE_ENTRIES, SURFACE_MINIAPP, course_for_pool, public_catalog
from .auth import require_user
from .quiz import (
    MODE_CONFIG,
    answer_quiz,
    cancel_quiz,
    get_active_quiz,
    get_current_question,
    get_miniapp_history,
    prepare_question,
    public_question,
    question_id,
)
from .quiz_start import start_quiz
from .ttl_cache import TTLValueCache

logger = logging.getLogger(__name__)
BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "miniapp"
STARTED_AT = datetime.now(UTC)
_DB_READY_CACHE = TTLValueCache[bool](ttl_seconds=2)
_TOTAL_USERS_CACHE = TTLValueCache[int](ttl_seconds=15)
_PUBLIC_USER_FIELDS = frozenset(
    {
        "username", "first_name", "first_play_date", "created_at", "last_activity",
        "total_points", "total_tests", "total_questions_answered", "total_correct_answers",
        "total_time_spent", "battles_played", "battles_won", "battles_lost", "battles_draw",
        "perfect_count", "max_streak_ever", "daily_streak", "daily_streak_last",
        "daily_activity_streak", "daily_activity_last", "challenge_streak_count",
        "challenge_streak_last_date", "achievements",
    }
)
_PUBLIC_LEVEL_SUFFIXES = ("attempts", "correct", "total", "best_score")
_PUBLIC_MODE_LABELS = {
    "relaxed": "🧘 Спокойный",
    "timed": "⏱ На время",
    "speed": "⚡ Скоростной",
}


def _uptime_seconds() -> int:
    return max(0, int((datetime.now(UTC) - STARTED_AT).total_seconds()))


def _json_error(message: str, status: int):
    return jsonify({"error": message}), status


def _database_ready() -> bool:
    try:
        from database import check_db_connection
        return bool(_DB_READY_CACHE.get(check_db_connection))
    except Exception:
        return False


def _total_users() -> int:
    try:
        from database import get_total_users
        return int(_TOTAL_USERS_CACHE.get(get_total_users))
    except Exception:
        return 0


def _public_user_document(document: dict | None) -> dict:
    allowed = set(_PUBLIC_USER_FIELDS)
    level_keys = {entry.pool_key for entry in COURSE_ENTRIES}
    try:
        from database import ALL_LEVEL_KEYS
        level_keys.update(ALL_LEVEL_KEYS)
    except Exception:
        pass
    allowed.update(
        f"{level_key}_{suffix}"
        for level_key in level_keys
        for suffix in _PUBLIC_LEVEL_SUFFIXES
    )
    return {key: value for key, value in (document or {}).items() if key in allowed}


def _history_timestamp(item: dict) -> float:
    value = item.get("end_time") or item.get("finished_at_dt") or item.get("updated_at_dt")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.timestamp()
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.timestamp()
        except ValueError:
            return 0.0
    return 0.0


def _serialize_history(items: list[dict]) -> list[dict]:
    result = []
    for item in items:
        clean = {}
        for key, value in item.items():
            if key.startswith("_"):
                continue
            clean[key] = value.isoformat() if hasattr(value, "isoformat") else value
        result.append(clean)
    return result


def _hard_leaderboard(limit: int = 20) -> list[dict]:
    try:
        from database import collection
        if collection is None:
            return []
        hard_keys = ("hard", "hard_p1", "hard_p2")
        pipeline = [
            {"$match": {"$or": [{f"{key}_attempts": {"$gt": 0}} for key in hard_keys]}},
            {"$addFields": {
                "_hard_correct": {"$add": [{"$ifNull": [f"${key}_correct", 0]} for key in hard_keys]},
                "_hard_total": {"$add": [{"$ifNull": [f"${key}_total", 0]} for key in hard_keys]},
            }},
            {"$sort": {"_hard_correct": -1, "_hard_total": 1}},
            {"$limit": max(1, min(int(limit), 100))},
        ]
        return list(collection.aggregate(pipeline))
    except Exception:
        logger.exception("hard leaderboard aggregation failed")
        return []


def _public_mode_catalog() -> dict[str, dict]:
    modes: dict[str, dict] = {}
    for mode, config in MODE_CONFIG.items():
        time_limit = config.get("time_limit")
        detail = f"{int(time_limit)} сек" if time_limit else "без таймера"
        modes[mode] = {
            "id": mode,
            "label": _PUBLIC_MODE_LABELS.get(mode, mode),
            "description": detail,
            "time_limit": time_limit,
        }
    return modes


def _attach_course_key(body: dict | None) -> dict | None:
    """Enrich a normal active-session response for old sessions without course_key."""
    if not isinstance(body, dict) or body.get("challenge"):
        return body
    if body.get("course_key"):
        return body
    pool_key = str(body.get("pool_key", ""))
    if not pool_key:
        return body
    try:
        entry = course_for_pool(pool_key, surface=SURFACE_MINIAPP)
    except Exception:
        entry = None
    if entry is not None:
        body["course_key"] = entry.key
    return body


def create_app() -> Flask:
    app = Flask(__name__, static_folder=str(STATIC_DIR) if STATIC_DIR.is_dir() else None, static_url_path="")

    @app.get("/live")
    def live():
        return jsonify({"status": "ok", "uptime_seconds": _uptime_seconds()})

    @app.get("/ready")
    def ready():
        db_ok = _database_ready()
        return jsonify({"status": "ready" if db_ok else "not_ready", "database": db_ok}), 200 if db_ok else 503

    @app.get("/health")
    def health():
        db_ok = _database_ready()
        return jsonify({"status": "healthy" if db_ok else "degraded", "database": "connected" if db_ok else "unavailable", "uptime_seconds": _uptime_seconds()})

    @app.get("/")
    def home():
        index = STATIC_DIR / "index.html"
        if index.is_file():
            return send_file(index)
        return _json_error("miniapp is not installed", 404)

    @app.get("/app")
    @app.get("/miniapp")
    @app.get("/miniapp/")
    def miniapp_root():
        return home()

    @app.get("/miniapp/<path:path>")
    @app.get("/app/<path:path>")
    def miniapp_static(path: str):
        if (STATIC_DIR / path).is_file():
            return send_from_directory(str(STATIC_DIR), path)
        return home()

    @app.get("/api/catalog")
    def catalog():
        """Public, deployment-fresh learning catalog; no question/source internals."""
        try:
            body = public_catalog(surface=SURFACE_MINIAPP)
            body["modes"] = _public_mode_catalog()
            response = jsonify(body)
            # A pool can appear/disappear only with a server deploy. Avoid a
            # browser/proxy keeping the previous deployment's availability.
            response.headers["Cache-Control"] = "no-store, max-age=0"
            return response
        except Exception:
            logger.exception("course catalog unavailable")
            return _json_error("course catalog unavailable", 503)

    @app.get("/stats")
    @app.get("/api/stats")
    def stats():
        try:
            from questions import POOL_REGISTRY
            pools = {key: len(value) for key, value in POOL_REGISTRY.items()}
            unique_ids = {
                question_id(question)
                for key, questions in POOL_REGISTRY.items()
                if key != "random_all"
                for question in questions
            }
            return jsonify({"status": "ok", "database": "connected" if _database_ready() else "unavailable", "total_users": _total_users(), "total_questions": len(unique_ids), "pools": pools, "uptime_seconds": _uptime_seconds()})
        except Exception:
            logger.exception("stats endpoint failed")
            return _json_error("stats unavailable", 503)

    @app.get("/api/botinfo")
    def botinfo():
        return jsonify({"username": os.getenv("BOT_USERNAME", "").lstrip("@")})

    @app.get("/api/pools")
    def pools():
        try:
            from questions import POOL_REGISTRY
            return jsonify({key: len(value) for key, value in POOL_REGISTRY.items()})
        except Exception:
            logger.exception("pool registry unavailable")
            return _json_error("question pools unavailable", 503)

    @app.get("/api/questions/<pool_key>")
    def questions(pool_key: str):
        try:
            from questions import get_pool_by_key
            pool = get_pool_by_key(pool_key)
            sample = random.sample(pool, min(10, len(pool)))
            return jsonify([public_question(prepare_question(question)) for question in sample])
        except KeyError:
            return _json_error("unknown question pool", 404)
        except Exception:
            logger.exception("question endpoint failed")
            return _json_error("questions unavailable", 503)

    @app.get("/api/quiz/active")
    def quiz_active():
        user, error = require_user()
        if error:
            return error
        body, message, status = get_active_quiz(user)
        body = _attach_course_key(body)
        return jsonify(body) if body is not None else _json_error(message, status)

    @app.post("/api/quiz/start")
    def quiz_start():
        user, error = require_user()
        if error:
            return error
        body, message, status = start_quiz(user, request.get_json(silent=True) or {})
        return jsonify(body) if body is not None else _json_error(message, status)

    @app.post("/api/quiz/current")
    def quiz_current():
        user, error = require_user()
        if error:
            return error
        body, message, status = get_current_question(user, request.get_json(silent=True) or {})
        return jsonify(body) if body is not None else _json_error(message, status)

    @app.post("/api/quiz/answer")
    def quiz_answer():
        user, error = require_user()
        if error:
            return error
        body, message, status = answer_quiz(user, request.get_json(silent=True) or {})
        return jsonify(body) if body is not None else _json_error(message, status)

    @app.post("/api/quiz/cancel")
    def quiz_cancel():
        user, error = require_user()
        if error:
            return error
        body, message, status = cancel_quiz(user, request.get_json(silent=True) or {})
        return jsonify(body) if body is not None else _json_error(message, status)

    @app.get("/api/me")
    def me():
        user, error = require_user()
        if error:
            return error
        uid = int(user["id"])
        try:
            from database import get_user_achievements, get_user_history, get_user_position, get_user_stats
            position, entry = get_user_position(uid)
            stats_data = get_user_stats(uid) or {}
            bot_history = get_user_history(uid, limit=10)
            miniapp_history = get_miniapp_history(uid, limit=10)
            history = sorted(miniapp_history + bot_history, key=_history_timestamp, reverse=True)[:10]
            achievements, streak_count, streak_date = get_user_achievements(uid)
            return jsonify({"user": user, "position": position, "entry": _public_user_document(entry), "stats": _public_user_document(stats_data), "history": _serialize_history(history), "achievements": achievements, "streak": {"count": streak_count, "last": streak_date}})
        except Exception:
            logger.exception("api/me failed")
            return _json_error("profile unavailable", 503)

    @app.get("/api/leaderboard")
    def leaderboard():
        _user, error = require_user()
        if error:
            return error
        category = request.args.get("cat", "general")
        if category not in {"general", "context", "hard"}:
            return _json_error("invalid leaderboard category", 400)
        try:
            from database import get_context_leaderboard, get_leaderboard_page
            if category == "general":
                raw_users = get_leaderboard_page(0, per_page=20)
                score_key = "total_points"
            elif category == "context":
                raw_users = get_context_leaderboard(limit=20)
                score_key = "_context_correct"
            else:
                raw_users = _hard_leaderboard(limit=20)
                score_key = "_hard_correct"
            users = [{"rank": rank, "username": item.get("username", ""), "first_name": item.get("first_name", "Пользователь"), "score": item.get(score_key, 0), "total_tests": item.get("total_tests", 0)} for rank, item in enumerate(raw_users, start=1)]
            return jsonify({"cat": category, "users": users})
        except Exception:
            logger.exception("leaderboard endpoint failed")
            return _json_error("leaderboard unavailable", 503)

    return app
