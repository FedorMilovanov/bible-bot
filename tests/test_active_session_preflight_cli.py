import json

import scripts.check_active_session_duplicates as preflight


def test_preflight_is_zero_when_no_duplicates(monkeypatch, capsys):
    monkeypatch.setattr(
        preflight,
        "find_duplicate_active_session_users",
        lambda limit: [],
    )

    assert preflight.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"ok": True, "duplicate_active_sessions": 0}


def test_preflight_reports_duplicates_without_mutating(monkeypatch, capsys):
    rows = [
        {"user_id": "42", "count": 2},
        {"user_id": "99", "count": 3},
    ]
    seen = {}

    def find(limit):
        seen["limit"] = limit
        return rows

    monkeypatch.setattr(preflight, "find_duplicate_active_session_users", find)

    assert preflight.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert seen["limit"] == 500
    assert payload["error"] == "duplicate_active_sessions"
    assert payload["users"] == rows


def test_preflight_outage_is_distinct_failure(monkeypatch, capsys):
    def unavailable(limit):
        raise preflight.QuizSessionAccessUnavailable("mongo down")

    monkeypatch.setattr(preflight, "find_duplicate_active_session_users", unavailable)

    assert preflight.main() == 2
    captured = capsys.readouterr()
    payload = json.loads(captured.err)
    assert payload["error"] == "preflight_unavailable"
    assert "mongo down" in payload["detail"]
