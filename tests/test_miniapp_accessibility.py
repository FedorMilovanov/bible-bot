from pathlib import Path


HTML = (Path(__file__).resolve().parents[1] / "miniapp" / "index.html").read_text(encoding="utf-8")


def test_viewport_does_not_disable_user_zoom():
    assert 'name="viewport"' in HTML
    assert "user-scalable=no" not in HTML
    assert "maximum-scale=1" not in HTML


def test_dynamic_status_regions_are_announced_politely():
    assert 'id="quizFeedback" class="feedback hidden" role="status" aria-live="polite"' in HTML
    assert 'id="toast" class="toast hidden" role="status" aria-live="polite"' in HTML
    assert 'id="lbList" class="lb-list" aria-live="polite"' in HTML


def test_interactive_static_buttons_have_explicit_type():
    assert '<button id="streakBadge" class="streak hidden" type="button">' in HTML
    assert '<button id="openBotBtn" class="btn btn-outline" type="button">' in HTML
    assert '<button class="btn btn-primary" id="resultReview" type="button">' in HTML
