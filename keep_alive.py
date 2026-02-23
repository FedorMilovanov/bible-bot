# keep_alive.py
"""
HTTP-сервер для поддержания активности на Render / Railway / Replit.
Отдаёт health-check endpoint и базовую статистику.
"""

import os
import time
import logging
from datetime import datetime, timezone
from threading import Thread
from flask import Flask, jsonify

logger = logging.getLogger(__name__)

app = Flask(__name__)

# Время старта — для uptime
_start_time = time.time()
_start_dt = datetime.now(timezone.utc).isoformat()


@app.route("/")
def home():
    """Простой health-check — для UptimeRobot / cron-job / Render."""
    uptime_sec = int(time.time() - _start_time)
    hours, remainder = divmod(uptime_sec, 3600)
    minutes, seconds = divmod(remainder, 60)
    return jsonify({
        "status": "alive",
        "uptime": f"{hours}h {minutes}m {seconds}s",
        "started_at": _start_dt,
    })


@app.route("/health")
def health():
    """Расширенный health-check — проверяет MongoDB."""
    from database import check_db_connection
    db_ok = check_db_connection()
    status_code = 200 if db_ok else 503
    return jsonify({
        "status": "healthy" if db_ok else "degraded",
        "database": "connected" if db_ok else "unavailable",
        "uptime_seconds": int(time.time() - _start_time),
    }), status_code


@app.route("/stats")
def stats():
    """Краткая статистика — для мониторинга (не раскрывает чувствительные данные)."""
    try:
        from database import get_total_users, check_db_connection
        from questions import get_total_question_count, get_all_pool_stats
        return jsonify({
            "status": "alive",
            "database": "connected" if check_db_connection() else "unavailable",
            "total_users": get_total_users(),
            "total_questions": get_total_question_count(),
            "pools": get_all_pool_stats(),
            "uptime_seconds": int(time.time() - _start_time),
        })
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 500


def run():
    """Запускает Flask в продакшн-режиме (без дебага)."""
    port = int(os.getenv("PORT", 8080))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,  # не перезапускать — мы внутри бота
    )


def keep_alive():
    """Запускает HTTP-сервер в фоновом потоке."""
    t = Thread(target=run, daemon=True, name="KeepAliveServer")
    t.start()
    logger.info("🌐 Keep-alive сервер запущен на порту %s",
                os.getenv("PORT", 8080))
