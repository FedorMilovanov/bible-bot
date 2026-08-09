from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HTML = (ROOT / "miniapp" / "index.html").read_text(encoding="utf-8")
LIFECYCLE = (ROOT / "miniapp" / "lifecycle.js").read_text(encoding="utf-8")


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


def test_closing_confirmation_is_scoped_to_active_quiz():
    assert '<script src="lifecycle.js"></script>' in HTML
    assert "screen-quiz" in LIFECYCLE
    assert "enableClosingConfirmation()" in LIFECYCLE
    assert "disableClosingConfirmation()" in LIFECYCLE
