from __future__ import annotations

from course_catalog import SURFACE_MINIAPP, list_courses
from web_api.routes import _public_user_document, create_app


def _course(payload: dict, key: str) -> dict:
    return next(
        course
        for group in payload["groups"]
        for course in group["courses"]
        if course["key"] == key
    )


def _keys(payload: dict) -> set[str]:
    return {
        course["key"]
        for group in payload["groups"]
        for course in group["courses"]
    }


def test_public_catalog_is_server_authoritative_and_not_cached():
    app = create_app()
    client = app.test_client()

    response = client.get("/api/catalog")
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store, max-age=0"

    payload = response.get_json()
    chapter2 = _course(payload, "chapter2")
    chapter3 = _course(payload, "chapter3")
    assert chapter2["scoring_mode"] == "learning"
    assert chapter3["scoring_mode"] == "learning"
    assert chapter2["points_per_question"] == 0
    assert chapter3["points_per_question"] == 0

    expected = {entry.key for entry in list_courses(surface=SURFACE_MINIAPP)}
    assert _keys(payload) == expected


def test_public_catalog_does_not_leak_question_or_competitive_internals():
    payload = create_app().test_client().get("/api/catalog").get_json()
    forbidden = {
        "pool_key",
        "ranked",
        "correct",
        "correct_answer",
        "questions",
        "source",
        "sources",
        "ranking_authorized_ids",
        "session_id",
        "persistence_id",
    }
    for group in payload["groups"]:
        for course in group["courses"]:
            assert forbidden.isdisjoint(course)


def test_mode_metadata_is_descriptive_and_does_not_expose_scoring_multiplier():
    payload = create_app().test_client().get("/api/catalog").get_json()
    assert set(payload["modes"]) == {"relaxed", "timed", "speed"}
    assert payload["modes"]["relaxed"]["time_limit"] is None
    assert payload["modes"]["timed"]["time_limit"] > 0
    assert payload["modes"]["speed"]["time_limit"] > 0
    for mode in payload["modes"].values():
        assert "score_multiplier" not in mode
        assert chr(0xD7) not in mode["description"]


def test_public_profile_exposes_catalog_chapter_progress_without_legacy_level_registration():
    document = {
        "total_points": 42,
        "chapter2_attempts": 3,
        "chapter2_correct": 24,
        "chapter2_total": 30,
        "chapter2_best_score": 9,
        "chapter3_attempts": 1,
        "chapter4_attempts": 2,
        "legacy_learning_receipts": {"server-only": {}},
        "secret": "do not expose",
    }

    public = _public_user_document(document)

    assert public["total_points"] == 42
    assert public["chapter2_attempts"] == 3
    assert public["chapter2_correct"] == 24
    assert public["chapter2_total"] == 30
    assert public["chapter2_best_score"] == 9
    assert public["chapter3_attempts"] == 1
    assert public["chapter4_attempts"] == 2
    assert "legacy_learning_receipts" not in public
    assert "secret" not in public
