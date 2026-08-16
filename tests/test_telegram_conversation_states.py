from pathlib import Path
from types import SimpleNamespace

import pytest

import telegram_conversation_states as states


ROOT = Path(__file__).resolve().parents[1]
COURSE_SOURCE = (ROOT / "telegram_course_surface.py").read_text(encoding="utf-8")


def test_canonical_quiz_conversation_state_values_preserve_deployed_contract():
    assert (states.CHOOSING_LEVEL, states.ANSWERING, states.BATTLE_ANSWERING) == (0, 1, 2)


def test_legacy_state_validator_accepts_exact_values():
    legacy = SimpleNamespace(
        CHOOSING_LEVEL=0,
        ANSWERING=1,
        BATTLE_ANSWERING=2,
    )
    states.validate_legacy_quiz_states(legacy)


@pytest.mark.parametrize(
    "name,value",
    [
        ("CHOOSING_LEVEL", 9),
        ("ANSWERING", 9),
        ("BATTLE_ANSWERING", 9),
    ],
)
def test_legacy_state_validator_fails_closed_on_drift(name, value):
    values = {
        "CHOOSING_LEVEL": 0,
        "ANSWERING": 1,
        "BATTLE_ANSWERING": 2,
    }
    values[name] = value
    with pytest.raises(RuntimeError, match="conversation states diverged"):
        states.validate_legacy_quiz_states(SimpleNamespace(**values))


def test_course_surface_uses_canonical_states_without_hidden_bot_import():
    assert "from telegram_conversation_states import ANSWERING, CHOOSING_LEVEL" in COURSE_SOURCE
    assert '__import__("bot")' not in COURSE_SOURCE
    assert "import bot" not in COURSE_SOURCE
    assert "from bot" not in COURSE_SOURCE
    assert "return CHOOSING_LEVEL" in COURSE_SOURCE
    assert "return ANSWERING" in COURSE_SOURCE
