#!/usr/bin/env python3
"""Container-level production smoke for the built web image.

The CI workflow runs this against the production image connected to an ephemeral
Mongo container. No real Telegram or production credentials are used.
"""
from __future__ import annotations

import json
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen


BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:18080").rstrip("/")
DEBUG_HEADERS = {"X-Debug-User-Id": "424242"}


def _request(
    path: str,
    *,
    method: str = "GET",
    payload: object | None = None,
    headers: dict[str, str] | None = None,
    expected: int = 200,
):
    data = None
    request_headers = dict(headers or {})
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    request = Request(
        f"{BASE}{path}",
        data=data,
        headers=request_headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=10) as response:
            status = response.status
            raw = response.read()
            content_type = response.headers.get("Content-Type", "")
    except HTTPError as exc:
        status = exc.code
        raw = exc.read()
        content_type = exc.headers.get("Content-Type", "")

    if status != expected:
        raise AssertionError(
            f"{method} {path}: expected HTTP {expected}, got {status}: {raw[:500]!r}"
        )
    if "application/json" in content_type:
        return json.loads(raw.decode("utf-8"))
    return raw.decode("utf-8")


def _assert_public_question(question: dict) -> None:
    assert set(question) <= {"id", "question", "options"}, question
    assert {"id", "question", "options"} <= set(question), question
    forbidden = {
        "correct",
        "correct_index",
        "correct_answer",
        "explanation",
        "verse",
        "topic",
        "source",
        "sources",
    }
    assert forbidden.isdisjoint(question), question


def main() -> None:
    live = _request("/live")
    assert live["status"] == "ok"

    ready = _request("/ready")
    assert ready == {"status": "ready", "database": True}

    # Web-only cold-start state must fail closed until the PTB bridge is ready.
    telegram_ready = _request("/telegram/ready", expected=503)
    assert telegram_ready["status"] == "not_ready"
    assert telegram_ready["transport"] == "webhook"

    # Wrong secret is rejected before attacker-controlled JSON is parsed.
    bad_secret = Request(
        f"{BASE}/telegram/webhook",
        data=b"{ definitely-not-json",
        headers={
            "Content-Type": "application/json",
            "X-Telegram-Bot-Api-Secret-Token": "definitely-wrong-secret",
        },
        method="POST",
    )
    try:
        urlopen(bad_secret, timeout=10)
    except HTTPError as exc:
        assert exc.code == 401
        body = json.loads(exc.read().decode("utf-8"))
        assert body["error"] == "invalid telegram webhook secret"
    else:
        raise AssertionError("wrong webhook secret was accepted")

    catalog = _request("/api/catalog")
    assert catalog["version"] == 1
    courses = [course for group in catalog["groups"] for course in group["courses"]]
    keys = [course["key"] for course in courses]
    assert len(keys) == len(set(keys))
    assert {"chapter2", "chapter3"} <= set(keys)
    forbidden_catalog = {
        "pool_key",
        "ranked",
        "correct",
        "correct_answer",
        "questions",
        "source",
        "sources",
        "ranking_authorized_ids",
        "persistence_id",
        "session_id",
        "multiplier",
        "score_multiplier",
    }
    for course in courses:
        assert forbidden_catalog.isdisjoint(course), course

    home = _request("/")
    assert "<title>Библейский тест — 1 Петра</title>" in home
    assert '<script src="course_catalog.js"></script>' in home
    course_asset = _request("/course_catalog.js")
    assert "buildCourseStartPayload" in course_asset

    public_questions = _request("/api/questions/easy_p1", headers=DEBUG_HEADERS)
    assert public_questions
    for question in public_questions:
        _assert_public_question(question)

    # Ambiguous client-side policy-looking fields fail closed.
    override = _request(
        "/api/quiz/start",
        method="POST",
        payload={
            "course_key": "chapter2",
            "mode": "relaxed",
            "count": 10,
            "pool": "competitive_all",
            "multiplier": 999,
        },
        headers=DEBUG_HEADERS,
        expected=400,
    )
    assert "client cannot override" in override["error"]

    started = _request(
        "/api/quiz/start",
        method="POST",
        payload={"course_key": "level_easy_p1", "mode": "relaxed", "count": 10},
        headers=DEBUG_HEADERS,
    )
    assert started["active"] is True
    assert started["course_key"] == "level_easy_p1"
    session_id = started["session_id"]
    question = started["question"]
    _assert_public_question(question)

    final = None
    for _index in range(10):
        answer = _request(
            "/api/quiz/answer",
            method="POST",
            payload={
                "session_id": session_id,
                "question_id": question["id"],
                "chosen": 0,
            },
            headers=DEBUG_HEADERS,
        )
        if answer["finished"]:
            final = answer
            break
        current = _request(
            "/api/quiz/current",
            method="POST",
            payload={"session_id": session_id},
            headers=DEBUG_HEADERS,
        )
        question = current["question"]
        _assert_public_question(question)

    assert final is not None
    assert final["finished"] is True
    assert final["total"] == 10
    assert isinstance(final["score"], int)
    assert isinstance(final["points"], int)

    profile = _request("/api/me", headers=DEBUG_HEADERS)
    assert profile["entry"]["total_tests"] == 1
    assert profile["entry"]["easy_p1_attempts"] == 1

    print("container-e2e-ok")


if __name__ == "__main__":
    main()
