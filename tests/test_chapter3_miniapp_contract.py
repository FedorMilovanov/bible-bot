from pathlib import Path

from course_catalog import SURFACE_MINIAPP, public_catalog, resolve_course
from questions.pool_policy import get_pool_policy

ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / "miniapp" / "index.html").read_text(encoding="utf-8")
APP_JS = (ROOT / "miniapp" / "app.js").read_text(encoding="utf-8")
CATALOG_JS = (ROOT / "miniapp" / "course_catalog.js").read_text(encoding="utf-8")


def test_miniapp_exposes_chapter3_through_server_catalog_not_chapter_script():
    payload = public_catalog(surface=SURFACE_MINIAPP)
    chapter3 = next(
        course
        for group in payload["groups"]
        for course in group["courses"]
        if course["key"] == "chapter3"
    )
    assert chapter3["title"] == "📙 1 Петра — Глава 3"
    assert chapter3["scoring_mode"] == "learning"
    assert chapter3["points_per_question"] == 0
    assert '<script src="course_catalog.js"></script>' in INDEX
    assert "chapter3.js" not in INDEX


def test_chapter3_normal_course_is_full_learning_pool_and_non_ranked():
    entry = resolve_course("chapter3", surface=SURFACE_MINIAPP)
    policy = get_pool_policy(entry.pool_key)
    assert entry.pool_key == "chapter3"
    assert policy.scoring_mode == "learning"
    assert policy.ranked is False
    assert policy.points_per_question == 0


def test_chapter3_ui_cannot_add_challenge_or_ranked_override():
    assert "buildCourseStartPayload" in APP_JS
    assert "course_key: course.key" in CATALOG_JS
    assert "pool_key: course.pool_key" not in CATALOG_JS
    assert "ranked:" not in CATALOG_JS
    assert "scoring_mode:" not in CATALOG_JS
    assert "challenge: false" in CATALOG_JS
