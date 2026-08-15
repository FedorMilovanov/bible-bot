from __future__ import annotations

from course_catalog import SURFACE_MINIAPP, resolve_course
from web_api.quiz_start import _resolve_normal_course, start_quiz

USER = {"id": 123, "username": "course-test", "first_name": "Course"}


def test_normal_start_resolves_course_to_server_pool():
    entry = _resolve_normal_course({"course_key": "chapter3"}, "relaxed")
    assert entry == resolve_course("chapter3", surface=SURFACE_MINIAPP, mode="relaxed")
    assert entry.pool_key == "chapter3"


def test_legacy_pool_payload_only_maps_to_exposed_course():
    entry = _resolve_normal_course({"pool_key": "chapter2"}, "relaxed")
    assert entry.key == "chapter2"
    assert entry.pool_key == "chapter2"


def test_client_cannot_override_ranked_or_scoring_policy():
    for field, value in (
        ("ranked", True),
        ("scoring_mode", "scored"),
        ("points_per_question", 999),
        ("score_multiplier", 99),
    ):
        body, message, status = start_quiz(
            USER,
            {"course_key": "chapter3", "mode": "relaxed", field: value},
        )
        assert body is None
        assert status == 400
        assert "override server course policy" in message


def test_arbitrary_pool_key_cannot_bypass_course_catalog():
    body, message, status = start_quiz(
        USER,
        {"pool_key": "competitive_all", "mode": "relaxed"},
    )
    assert body is None
    assert status == 400
    assert "not an exposed Mini App course" in message


def test_unknown_course_is_rejected_before_session_creation():
    body, message, status = start_quiz(
        USER,
        {"course_key": "chapter999", "mode": "relaxed"},
    )
    assert body is None
    assert status == 400
    assert "unknown course" in message


def test_registered_chapter4_and_chapter5_resolve_to_canonical_pools():
    for course_key in ("chapter4", "chapter5"):
        entry = _resolve_normal_course({"course_key": course_key}, "relaxed")
        assert entry == resolve_course(course_key, surface=SURFACE_MINIAPP, mode="relaxed")
        assert entry.pool_key == course_key


def test_normal_course_rejects_client_question_count_override():
    body, message, status = start_quiz(
        USER,
        {"course_key": "chapter2", "mode": "relaxed", "count": 20},
    )
    assert body is None
    assert status == 400
    assert message == "question count must be 10"


def test_challenge_cannot_be_pointed_at_chapter3_or_receive_course_key():
    body, message, status = start_quiz(
        USER,
        {"pool_key": "chapter3", "mode": "relaxed", "challenge": True},
    )
    assert body is None
    assert status == 400
    assert message == "challenge requires random_all pool"

    body, message, status = start_quiz(
        USER,
        {
            "pool_key": "random_all",
            "course_key": "chapter3",
            "mode": "relaxed",
            "challenge": True,
        },
    )
    assert body is None
    assert status == 400
    assert message == "challenge does not accept course_key"
