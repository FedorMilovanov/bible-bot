import asyncio
import os
import threading
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("ADMIN_USER_ID", "1")

import telegram_report_controller as reports
import telegram_report_state as report_state


SOURCE = Path(reports.__file__).read_text(encoding="utf-8")
STATE_SOURCE = Path(report_state.__file__).read_text(encoding="utf-8")


def run(coro):
    return asyncio.run(coro)


def test_report_controller_has_no_legacy_module_dependency():
    assert "import bot" not in SOURCE
    assert "legacy." not in SOURCE
    assert "get_active_quiz_session_strict" in SOURCE
    assert "from database import can_submit_report, seconds_until_next_report" in SOURCE
    assert "from utils import safe_edit, safe_send" in SOURCE
    assert "main_menu.main_keyboard()" in SOURCE


def test_report_sanitizer_preserves_deployed_contract():
    raw = "  *bold* _under_ `code` [link] " + "x" * 2100
    sanitized = reports._sanitize_report_text(raw)

    assert sanitized.startswith(r"\*bold\* \_under\_ \`code\` \[link\]")
    assert len(sanitized) <= 2000 + 8
    assert not sanitized.startswith(" ")
    assert not sanitized.endswith(" ")


def test_report_state_constants_and_drafts_have_one_canonical_owner():
    assert (
        report_state.REPORT_TYPE,
        report_state.REPORT_TEXT,
        report_state.REPORT_PHOTO,
        report_state.REPORT_CONFIRM,
    ) == (10, 11, 12, 13)
    assert report_state.REPORT_TYPE_LABELS == {
        "bug": "🐞 Баг",
        "idea": "💡 Идея",
        "question": "❓ Вопрос по материалу",
    }
    assert reports.report_drafts is report_state.report_drafts
    assert "install_legacy_bridge" not in STATE_SOURCE
    assert "legacy_module" not in STATE_SOURCE


class _Query:
    def __init__(self):
        self.data = "report_inaccuracy_0"
        self.answers = []

    async def answer(self, text=None, show_alert=False):
        self.answers.append((text, show_alert))


async def _noop_drain(*_args, **_kwargs):
    return None


def test_inaccuracy_uses_durable_session_and_worker_threads(monkeypatch):
    event_loop_thread = threading.get_ident()
    lookup_threads = []
    accept_threads = []

    def get_session(user_id):
        lookup_threads.append(threading.get_ident())
        assert user_id == 42
        return {
            "attempt_id": "attempt-1",
            "level_name": "Level",
            "questions_data": [{"question": "Q", "options": ["A"], "correct": 0}],
        }

    def accept(**kwargs):
        accept_threads.append(threading.get_ident())
        assert kwargs["attempt_id"] == "attempt-1"
        assert kwargs["question_index"] == 0
        return {"_id": "report-1"}

    monkeypatch.setattr(reports, "get_active_quiz_session_strict", get_session)
    monkeypatch.setattr(reports, "accept_inaccuracy_report_once", accept)
    monkeypatch.setattr(reports, "drain_report_outbox", _noop_drain)

    query = _Query()
    user = SimpleNamespace(id=42, username="u", first_name="F")
    update = SimpleNamespace(callback_query=query, effective_user=user)
    context = SimpleNamespace(bot=object())

    run(reports.report_inaccuracy_handler(update, context))

    assert query.answers == [("✅ Неточность сохранена.", False)]
    assert len(lookup_threads) == 1
    assert len(accept_threads) == 1
    assert lookup_threads[0] != event_loop_thread
    assert accept_threads[0] != event_loop_thread
