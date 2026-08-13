from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MINIAPP_START = (ROOT / "web_api" / "quiz_start.py").read_text(encoding="utf-8")
ROUTES = (ROOT / "web_api" / "routes.py").read_text(encoding="utf-8")
PRODUCTION = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
TELEGRAM_CHALLENGE = (ROOT / "telegram_challenge_controller.py").read_text(encoding="utf-8")


def test_miniapp_route_uses_competitive_start_adapter():
    assert "from .quiz_start import start_quiz" in ROUTES
    assert "questions.pick_competitive_challenge_questions(challenge_mode)" in MINIAPP_START
    assert 'challenge_mode = "hardcore20" if mode == "speed" else "random20"' in MINIAPP_START
    assert 'if pool_key != "random_all":' in MINIAPP_START


def test_telegram_production_uses_dedicated_competitive_controller():
    assert "import telegram_challenge_controller as challenge" in PRODUCTION
    assert "challenge.challenge_start" in PRODUCTION
    assert "challenge.restart_session_handler" in PRODUCTION
    assert TELEGRAM_CHALLENGE.count("pick_competitive_challenge_questions(mode)") == 2
