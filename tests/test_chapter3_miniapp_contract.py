from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / "miniapp" / "index.html").read_text(encoding="utf-8")
CHAPTER3_JS = (ROOT / "miniapp" / "chapter3.js").read_text(encoding="utf-8")


def test_miniapp_exposes_chapter3_learning_entry_and_script():
    assert 'data-action="chapter3"' in INDEX
    assert '<script src="chapter3.js"></script>' in INDEX
    assert "Глава 3" in INDEX
    assert "без рейтинга" in INDEX


def test_chapter3_entry_uses_server_quiz_pool_and_learning_copy():
    assert "startQuiz('chapter3', mode.id, 10, false)" in CHAPTER3_JS
    assert "1 Петра — Глава 3" in CHAPTER3_JS
    assert "Учебный режим без рейтинга" in CHAPTER3_JS
    assert "Спорные толкования и позиции курса" in CHAPTER3_JS


def test_chapter3_ui_does_not_add_challenge_or_battle_mode():
    assert "startQuiz('chapter3', mode.id, 10, true)" not in CHAPTER3_JS
    assert "random20" not in CHAPTER3_JS
    assert "hardcore20" not in CHAPTER3_JS
    assert "battle" not in CHAPTER3_JS.lower()
