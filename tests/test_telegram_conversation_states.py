from pathlib import Path

import telegram_conversation_states as states


ROOT = Path(__file__).resolve().parents[1]
COURSE_SOURCE = (ROOT / "telegram_course_surface.py").read_text(encoding="utf-8")
STATE_SOURCE = (ROOT / "telegram_conversation_states.py").read_text(encoding="utf-8")


def test_canonical_quiz_conversation_state_values_preserve_deployed_contract():
    assert (states.CHOOSING_LEVEL, states.ANSWERING, states.BATTLE_ANSWERING) == (0, 1, 2)


def test_conversation_states_have_no_retired_legacy_validator():
    assert not hasattr(states, "validate_legacy_quiz_states")
    assert "validate_legacy_quiz_states" not in STATE_SOURCE
    assert "legacy_module" not in STATE_SOURCE


def test_course_surface_uses_canonical_states_without_hidden_bot_import():
    assert "from telegram_conversation_states import ANSWERING, CHOOSING_LEVEL" in COURSE_SOURCE
    assert '__import__("bot")' not in COURSE_SOURCE
    assert "import bot" not in COURSE_SOURCE
    assert "from bot" not in COURSE_SOURCE
    assert "return CHOOSING_LEVEL" in COURSE_SOURCE
    assert "return ANSWERING" in COURSE_SOURCE
