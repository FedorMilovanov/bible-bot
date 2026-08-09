"""HTTP API package for the Telegram Mini App and Telegram webhook ingress."""
from __future__ import annotations

import hmac
import logging
import os

from flask import g, jsonify, request
from pymongo.errors import DuplicateKeyError, PyMongoError
from werkzeug.exceptions import BadRequest, RequestEntityTooLarge

from .auth import get_user_from_request
from .db_hardening import ensure_miniapp_indexes
from .rate_limit import GLOBAL_API_LIMITER
from .routes import create_app as _create_routes_app
from .telegram_transport import (
    TELEGRAM_WEBHOOK_BRIDGE,
    WEBHOOK_PATH,
    InvalidWebhookUpdate,
    TransportConfigurationError,
    WebhookNotReady,
    telegram_transport_mode,
    telegram_webhook_secret,
)
from .user_locks import user_operation_lock

logger = logging.getLogger(__name__)

_DEFAULT_SERVER_BODY_BYTES = 1024 * 1024
_DEFAULT_MINIAPP_BODY_BYTES = 64 * 1024

# Per authenticated Telegram user. Values are (requests, window seconds).
_RATE_LIMITS = {
    ("POST", "/api/quiz/start"): (12, 60),
    ("POST", "/api/quiz/current"): (180, 60),
    ("POST", "/api/quiz/answer"): (180, 60),
    ("GET", "/api/me"): (60, 60),
    ("GET", "/api/leaderboard"): (60, 60),
}
_QUESTION_ENDPOINT_LIMIT = (30, 60)
_QUESTION_RATE_SCOPE = "/api/questions/*"
_SERIALIZED_QUIZ_PATHS = frozenset({
    "/api/quiz/start",
    "/api/quiz/current",
    "/api/quiz/answer",
})


def create_app():
    app = _create_routes_app()
    # Waitress and Flask keep a bounded envelope large enough for Telegram
    # webhook Update objects. Mini App quiz JSON gets a much smaller per-request
    # cap below, using Flask 3.1's Request.max_content_length override.
    app.config["MAX_CONTENT_LENGTH"] = int(
        os.getenv("MAX_REQUEST_BODY_BYTES", str(_DEFAULT_SERVER_BODY_BYTES))
    )

    @app.get("/meta")
    def _deployment_meta():
        """Non-secret deployment identity for smoke checks and incident triage."""
        return jsonify(
            {
                "service": os.getenv("RENDER_SERVICE_NAME", "local"),
                "branch": os.getenv("RENDER_GIT_BRANCH", ""),
                "revision": os.getenv("RENDER_GIT_COMMIT", "")[:40],
                "environment": os.getenv("APP_ENV", "production"),
            }
        )

    @app.post(WEBHOOK_PATH)
    def _telegram_webhook():
        try:
            if telegram_transport_mode() != "webhook":
                return jsonify({"error": "not found"}), 404
            expected_secret = telegram_webhook_secret()
        except TransportConfigurationError:
            logger.exception("Telegram webhook transport configuration is invalid")
            return jsonify({"error": "telegram webhook unavailable"}), 503

        supplied_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if not supplied_secret or not hmac.compare_digest(supplied_secret, expected_secret):
            return jsonify({"error": "invalid telegram webhook secret"}), 401
        if not request.is_json:
            return jsonify({"error": "application/json required"}), 415

        try:
            payload = request.get_json(silent=False)
        except BadRequest:
            return jsonify({"error": "invalid JSON"}), 400
        if not isinstance(payload, dict):
            return jsonify({"error": "JSON object required"}), 400

        try:
            TELEGRAM_WEBHOOK_BRIDGE.submit(payload)
        except InvalidWebhookUpdate:
            return jsonify({"error": "invalid telegram update"}), 400
        except WebhookNotReady:
            return jsonify({"error": "telegram application not ready"}), 503
        except Exception:
            logger.exception("unexpected Telegram webhook dispatch failure")
            return jsonify({"error": "telegram webhook dispatch failed"}), 503
        return jsonify({"ok": True})

    @app.before_request
    def _protect_api_boundary():
        question_catalog_request = request.method == "GET" and request.path.startswith("/api/questions/")
        policy = _RATE_LIMITS.get((request.method, request.path))
        if policy is None and question_catalog_request:
            policy = _QUESTION_ENDPOINT_LIMIT
        if policy is None:
            return None

        # Authenticate protected API traffic before parsing request bodies.
        # The route itself still emits the canonical 401 response for its normal
        # authenticated surfaces. The legacy question compatibility route does
        # not own auth itself, so enforce it here as well.
        user = get_user_from_request()
        if not user:
            if question_catalog_request:
                return jsonify({"error": "telegram authentication required"}), 401
            return None

        limit, window_seconds = policy
        rate_scope = _QUESTION_RATE_SCOPE if question_catalog_request else request.path
        allowed, retry_after = GLOBAL_API_LIMITER.allow(
            f"{user['id']}:{request.method}:{rate_scope}",
            limit=limit,
            window_seconds=window_seconds,
        )
        if not allowed:
            response = jsonify({"error": "rate limit exceeded", "retry_after": retry_after})
            response.status_code = 429
            response.headers["Retry-After"] = str(retry_after)
            return response

        if request.path in _SERIALIZED_QUIZ_PATHS:
            request.max_content_length = int(
                os.getenv("MINIAPP_MAX_REQUEST_BODY_BYTES", str(_DEFAULT_MINIAPP_BODY_BYTES))
            )
            if not request.is_json:
                return jsonify({"error": "application/json required"}), 415
            try:
                payload = request.get_json(silent=False)
            except BadRequest:
                return jsonify({"error": "invalid JSON"}), 400
            if not isinstance(payload, dict):
                return jsonify({"error": "JSON object required"}), 400

            # Waitress is multi-threaded. Hold one bounded lock stripe for the
            # authenticated user until Flask tears down the request so a new
            # start cannot race that user's current/answer persistence.
            lock = user_operation_lock(user["id"])
            lock.acquire()
            g.miniapp_user_operation_lock = lock

        if request.path == "/api/quiz/start":
            # DB-level uniqueness complements the process-local same-user lock.
            # Index creation is lazy/retried and must not take Telegram polling
            # offline if MongoDB is temporarily unavailable.
            ensure_miniapp_indexes()
        return None

    @app.teardown_request
    def _release_user_operation_lock(_error):
        lock = getattr(g, "miniapp_user_operation_lock", None)
        if lock is not None:
            # Clear request-local ownership before releasing the shared stripe.
            # A repeated teardown cannot later release a lock re-acquired by a
            # different request that hashes to the same stripe.
            g.miniapp_user_operation_lock = None
            lock.release()

    @app.after_request
    def _security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Permitted-Cross-Domain-Policies"] = "none"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        if request.path.startswith("/api/") or request.path == WEBHOOK_PATH:
            response.headers["Cache-Control"] = "no-store"
            response.headers["Pragma"] = "no-cache"
        else:
            # Assets are not content-hashed yet, so avoid stale Mini App bundles.
            response.headers.setdefault("Cache-Control", "no-cache")
        return response

    @app.errorhandler(RequestEntityTooLarge)
    def _request_too_large(_error):
        return jsonify({"error": "request body too large"}), 413

    @app.errorhandler(DuplicateKeyError)
    def _duplicate_active_quiz(_error):
        return jsonify({"error": "another active quiz already exists; retry start"}), 409

    @app.errorhandler(PyMongoError)
    def _database_operation_failed(_error):
        return jsonify({"error": "database temporarily unavailable"}), 503

    return app


__all__ = ["create_app"]
