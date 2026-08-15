from datetime import datetime
import inspect

import database
from web_api import result_store


def test_database_week_id_uses_shared_utc_clock_at_iso_year_boundary(monkeypatch):
    fixed = datetime(2021, 1, 1, 23, 59, 59)
    monkeypatch.setattr(database, "_now_utc", lambda: fixed)

    assert database.get_current_week_id() == "2020-W53"
    assert database.get_current_week_id() == result_store._week_id_utc(fixed)


def test_database_week_id_rolls_to_next_iso_week_from_utc_authority(monkeypatch):
    fixed = datetime(2021, 1, 4, 0, 0, 0)
    monkeypatch.setattr(database, "_now_utc", lambda: fixed)

    assert database.get_current_week_id() == "2021-W01"
    assert database.get_current_week_id() == result_store._week_id_utc(fixed)


def test_database_week_id_no_longer_depends_on_host_local_date():
    source = inspect.getsource(database.get_current_week_id)

    assert "date.today" not in source
    assert "_now_utc().date()" in source
