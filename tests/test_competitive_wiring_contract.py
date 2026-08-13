from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MINIAPP_START = (ROOT / "web_api" / "quiz_start.py").read_text(encoding="utf-8")
ROUTES = (ROOT / "web_api" / "routes.py").read_text(encoding="utf-8")
PRODUCTION = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_miniapp_route_uses_competitive_start_adapter():
    assert "from .quiz_start import start_quiz" in ROUTES
    assert "questions.pick_competitive_challenge_questions(challenge_mode)" in MINIAPP_START
    assert 'challenge_mode = "hardcore20" if mode == "speed" else "random20"' in MINIAPP_START
    assert 'if pool_key != "random_all":' in MINIAPP_START


def test_telegram_production_uses_same_competitive_selector():
    assert "from questions import pick_competitive_challenge_questions" in PRODUCTION
    assert "legacy.pick_challenge_questions = pick_competitive_challenge_questions" in PRODUCTION
