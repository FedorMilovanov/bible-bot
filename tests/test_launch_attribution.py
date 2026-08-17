import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from web_api.launch_attribution import (
    COLLECTION_NAME,
    RETENTION_DAYS,
    SOURCE_TIME_INDEX_NAME,
    TTL_INDEX_NAME,
    parse_launch_param,
    persist_launch_attribution,
)


class FakeCollection:
    def __init__(self):
        self.indexes = {"_id_": {"key": [("_id", 1)]}}
        self.updates = []

    def create_index(self, keys, *, name, **options):
        spec = {"key": list(keys)}
        spec.update(options)
        self.indexes[name] = spec
        return name

    def index_information(self):
        return self.indexes

    def update_one(self, selector, update, *, upsert=False):
        self.updates.append((selector, update, upsert))
        return object()


class FakeDatabase:
    def __init__(self):
        self.collection = FakeCollection()

    def __getitem__(self, name):
        assert name == COLLECTION_NAME
        return self.collection


def test_python_parser_matches_shared_javascript_contract_fixtures():
    fixture_path = Path(__file__).parent / "fixtures" / "launch_params.json"
    fixtures = json.loads(fixture_path.read_text(encoding="utf-8"))

    for fixture in fixtures:
        parsed = parse_launch_param(fixture["raw"])
        assert {
            "kind": parsed.kind,
            "source": parsed.source,
            "destination": parsed.destination,
        } == {
            "kind": fixture["kind"],
            "source": fixture["source"],
            "destination": fixture["destination"],
        }, fixture["raw"]


def test_return_context_is_only_exposed_for_known_safe_site_sources():
    site = parse_launch_param("v1_site_app__home").public_dict()
    telegram = parse_launch_param("v1_tg_pin__home").public_dict()
    chapter = parse_launch_param("v1_site_ch2__chapter2").public_dict()

    assert site["return_context"] == {
        "kind": "site",
        "label": "Вернуться на сайт",
        "url": "https://gospod-bog.ru/app/",
    }
    assert telegram["return_context"] is None
    assert chapter["return_context"] is None


def test_persistence_is_idempotent_minimized_and_has_fixed_retention():
    database = FakeDatabase()
    context = parse_launch_param("v1_site_app__home")
    auth_date = 1_700_000_000
    now = datetime(2026, 8, 17, 12, 0, tzinfo=UTC)

    assert persist_launch_attribution(
        database=database,
        user_id=123456,
        auth_date=auth_date,
        query_id="query-secret",
        context=context,
        now=now,
    )
    assert persist_launch_attribution(
        database=database,
        user_id=123456,
        auth_date=auth_date,
        query_id="query-secret",
        context=context,
        now=now + timedelta(minutes=5),
    )

    first_selector, first_update, first_upsert = database.collection.updates[0]
    second_selector, second_update, second_upsert = database.collection.updates[1]
    assert first_selector == second_selector
    assert first_upsert is True and second_upsert is True
    assert "$setOnInsert" in first_update and "$inc" not in first_update
    assert "$setOnInsert" in second_update and "$inc" not in second_update

    document = first_update["$setOnInsert"]
    assert document["retention"]["expires_at"] == datetime.fromtimestamp(
        auth_date, UTC
    ) + timedelta(days=RETENTION_DAYS)
    assert document["first_seen_at"] == now
    assert document["source"] == "site_app"
    assert document["destination"] == "home"
    assert "user_id" not in document
    assert "query_id" not in document
    assert "init_data" not in document
    assert "hash" not in document
    assert "query-secret" not in json.dumps(document, default=str)
    assert "123456" not in json.dumps(document, default=str)

    assert database.collection.indexes[TTL_INDEX_NAME] == {
        "key": [("retention.expires_at", 1)],
        "expireAfterSeconds": 0,
    }
    assert database.collection.indexes[SOURCE_TIME_INDEX_NAME] == {
        "key": [("source", 1), ("first_seen_at", 1)],
    }


def test_legacy_and_invalid_launches_are_never_persisted():
    database = FakeDatabase()
    for raw in ("chapter2", "v2_site_app__home", "../home"):
        assert not persist_launch_attribution(
            database=database,
            user_id=123,
            auth_date=1_700_000_000,
            query_id="q",
            context=parse_launch_param(raw),
        )
    assert database.collection.updates == []
