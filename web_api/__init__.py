"""HTTP API package for the Telegram Mini App."""
from __future__ import annotations

import os

from flask import jsonify, request
from werkzeug.exceptions import RequestEntityTooLarge

from .auth import get_user_from_request
from .rate_limit import GLOBAL_API_LIMITER
from .routes import create_app as _create_routes_app

# Per authenticated Telegram user. Values are (requests, window seconds).
_RATE_LIMITS = {
    ("POST", "/api/quiz/start"): (12, 60),
    ("POST", "/api/quiz/current"): (180, 60),
    ("POST", "/api/quiz/answer"): (180, 60),
    ("GET", "/api/me"): (60, 60),
    ("GET", "/api/leaderboard"): (60, 60),
}


def create_app():
    app = _create_routes_app()
    app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_REQUEST_BODY_BYTES", str(64 * 1024)))

    @app.before_request
    def _protect_api_boundary():
        if request.path.startswith("/api/quiz/") and request.method == "POST" and not request.is_json:
            return jsonify({"error": "application/json required"}), 415

        policy = _RATE_LIMITS.get((request.method, request.path))
        if policy is None:
            return None

        # Let the route return the canonical 401 for invalid/missing Telegram auth.
        user = get_user_from_request()
        if not user:
            return None

        limit, window_seconds = policy
        allowed, retry_after = GLOBAL_API_LIMITER.allow(
            f"{user['id']}:{request.method}:{request.path}",
            limit=limit,
            window_seconds=window_seconds,
        )
        if allowed:
            return None

        response = jsonify({"error": "rate limit exceeded", "retry_after": retry_after})
        response.status_code = 429
        response.headers["Retry-After"] = str(retry_after)
        return response

    @app.after_request
    def _security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Permitted-Cross-Domain-Policies"] = "none"
        if request.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
            response.headers["Pragma"] = "no-cache"
        else:
            # Assets are not content-hashed yet, so avoid stale Mini App bundles.
            response.headers.setdefault("Cache-Control", "no-cache")
        return response

    @app.errorhandler(RequestEntityTooLarge)
    def _request_too_large(_error):
        return jsonify({"error": "request body too large"}), 413

    return app


__all__ = ["create_app"]
