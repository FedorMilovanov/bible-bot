from datetime import date

import database


class JanFirst2021(date):
    @classmethod
    def today(cls):
        return cls(2021, 1, 1)


class JanFourth2021(date):
    @classmethod
    def today(cls):
        return cls(2021, 1, 4)


def test_legacy_week_id_uses_iso_year_at_calendar_boundary(monkeypatch):
    monkeypatch.setattr(database, "date", JanFirst2021)
    assert database.get_current_week_id() == "2020-W53"


def test_legacy_week_id_starts_new_iso_year_on_first_iso_monday(monkeypatch):
    monkeypatch.setattr(database, "date", JanFourth2021)
    assert database.get_current_week_id() == "2021-W01"
