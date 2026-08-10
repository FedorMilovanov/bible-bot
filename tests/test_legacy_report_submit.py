from copy import deepcopy

import pytest

import legacy_report_submit as submit


def test_new_draft_gets_stable_id_before_content(monkeypatch):
    monkeypatch.setattr(submit, "new_report_id", lambda: "report-1")
    assert submit.new_report_draft("bug") == {
        "report_id": "report-1", "type": "bug", "text": None, "photo_file_id": None,
    }


def test_editing_content_preserves_report_identity(monkeypatch):
    monkeypatch.setattr(submit, "new_report_id", lambda: "report-1")
    draft = submit.new_report_draft("idea")
    submit.set_report_draft_text(draft, "  Improve this  ")
    submit.set_report_draft_photo(draft, "  file-123  ")
    assert draft == {
        "report_id": "report-1", "type": "idea",
        "text": "Improve this", "photo_file_id": "file-123",
    }


def test_acceptance_persists_photo_context_and_stable_id_without_mutating_draft(monkeypatch):
    draft = {
        "report_id": "report-1", "type": "question",
        "text": "What does this mean?", "photo_file_id": "photo-telegram-id",
    }
    before = deepcopy(draft)
    captured = {}
    monkeypatch.setattr(submit, "accept_report_once", lambda **kwargs: captured.update(kwargs) or {"_id": kwargs["report_id"]})
    result = submit.accept_report_draft_once(
        user_id=42, username="tester", first_name="Test", draft=draft,
        context={"mode": "easy", "q": 3},
    )
    assert result["_id"] == "report-1"
    assert captured["photo_file_id"] == "photo-telegram-id"
    assert captured["context"] == {"mode": "easy", "q": 3}
    assert captured["report_id"] == "report-1"
    assert draft == before


def test_failed_acceptance_leaves_draft_intact_for_retry(monkeypatch):
    draft = {"report_id": "report-1", "type": "bug", "text": "Failed", "photo_file_id": "photo-id"}
    before = deepcopy(draft)
    monkeypatch.setattr(submit, "accept_report_once", lambda **_: (_ for _ in ()).throw(RuntimeError("mongo unavailable")))
    with pytest.raises(RuntimeError, match="mongo unavailable"):
        submit.accept_report_draft_once(user_id=42, username=None, first_name="User", draft=draft)
    assert draft == before


def test_retry_reuses_same_report_id(monkeypatch):
    draft = {"report_id": "report-1", "type": "bug", "text": "Broken", "photo_file_id": None}
    seen = []
    monkeypatch.setattr(submit, "accept_report_once", lambda **kwargs: seen.append(kwargs["report_id"]) or {"_id": kwargs["report_id"]})
    for _ in range(2):
        submit.accept_report_draft_once(user_id=42, username="u", first_name="User", draft=draft)
    assert seen == ["report-1", "report-1"]


def test_snapshot_deep_copies_context():
    draft = {"report_id": "r", "type": "bug", "text": "Broken", "photo_file_id": None}
    context = {"nested": {"value": 1}}
    snapshot = submit.immutable_report_draft_snapshot(draft, context=context)
    context["nested"]["value"] = 2
    assert snapshot["context"] == {"nested": {"value": 1}}


def test_invalid_draft_fails_before_store(monkeypatch):
    monkeypatch.setattr(submit, "accept_report_once", lambda **_: pytest.fail("invalid draft must not reach store"))
    for draft in (
        {"report_id": "", "type": "bug", "text": "x"},
        {"report_id": "r", "type": "other", "text": "x"},
        {"report_id": "r", "type": "bug", "text": None},
        {"report_id": "r", "type": "bug", "text": "x", "photo_file_id": 123},
    ):
        with pytest.raises(submit.LegacyReportDraftInvalid):
            submit.accept_report_draft_once(user_id=42, username=None, first_name=None, draft=draft)
