"""Flask application for health, Mini App static files and API routes."""
from __future__ import annotations

import logging
import os
import pathlib
import random
from datetime import UTC, datetime

from flask import Flask, jsonify, request, send_file, send_from_directory

from .auth import require_user
from .quiz import (
    answer_quiz,
    get_current_question,
    get_miniapp_history,
    prepare_question,
    public_question,
    question_id,
    start_quiz,
)

logger = logging.getLogger(__name__)
BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "miniapp"
STARTED_AT = datetime.now(UTC)


def _uptime_seconds() -> int:
    return max(0, int((datetime.now(UTC) - STARTED_AT).total_seconds()))


def _json_error(message: str, status: int):
    return jsonify({"error": message}), status


def _database_ready() -> bool:
    try:
        from database import check_db_connection

        return bool(check_db_connection())
    except Exception:
        return False


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


def create_app() -> Flask:
    app = Flask(
        __name__,
        static_folder=str(STATIC_DIR) if STATIC_DIR.is_dir() else None,
        static_url_path="",
    )

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
        return jsonify(
            {
                "status": "healthy" if db_ok else "degraded",
                "database": "connected" if db_ok else "unavailable",
                "uptime_seconds": _uptime_seconds(),
            }
        )

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

    @app.get("/stats")
    @app.get("/api/stats")
    def stats():
        try:
            from database import get_total_users
            from questions import POOL_REGISTRY

            pools = {key: len(value) for key, value in POOL_REGISTRY.items()}
            unique_ids = {
                question_id(question)
                for key, questions in POOL_REGISTRY.items()
                if key != "random_all"
                for question in questions
            }
            return jsonify(
                {
                    "status": "ok",
                    "database": "connected" if _database_ready() else "unavailable",
                    "total_users": get_total_users(),
                    "total_questions": len(unique_ids),
                    "pools": pools,
                    "uptime_seconds": _uptime_seconds(),
                }
            )
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
        """Compatibility endpoint. It never exposes answers or explanations."""
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
            return jsonify(
                {
                    "user": user,
                    "position": position,
                    "entry": {k: v for k, v in (entry or {}).items() if not k.startswith("_")},
                    "stats": {k: v for k, v in stats_data.items() if not k.startswith("_")},
                    "history": _serialize_history(history),
                    "achievements": achievements,
                    "streak": {"count": streak_count, "last": streak_date},
                }
            )
        except Exception:
            logger.exception("api/me failed")
            return _json_error("profile unavailable", 503)

    @app.get("/api/leaderboard")
    def leaderboard():
        # Leaderboard rows contain user display names/usernames, so keep them
        # inside authenticated Telegram Mini App sessions rather than exposing
        # them to unauthenticated web scraping.
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

            users = [
                {
                    "rank": rank,
                    "username": item.get("username", ""),
                    "first_name": item.get("first_name", "Пользователь"),
                    "score": item.get(score_key, 0),
                    "total_tests": item.get("total_tests", 0),
                }
                for rank, item in enumerate(raw_users, start=1)
            ]
            return jsonify({"cat": category, "users": users})
        except Exception:
            logger.exception("leaderboard endpoint failed")
            return _json_error("leaderboard unavailable", 503)

    return app
