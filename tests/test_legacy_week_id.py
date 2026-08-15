from datetime import datetime

import database


def test_legacy_week_id_uses_iso_year_at_calendar_boundary(monkeypatch):
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2021, 1, 1, 12, 0, 0))
    assert database.get_current_week_id() == "2020-W53"


def test_legacy_week_id_starts_new_iso_year_on_first_iso_monday(monkeypatch):
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2021, 1, 4, 12, 0, 0))
    assert database.get_current_week_id() == "2021-W01"
