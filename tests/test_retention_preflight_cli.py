import inspect
import json
from copy import deepcopy
from pathlib import Path

import pytest

import scripts.check_retention_indexes as preflight


ROOT = Path(__file__).resolve().parents[1]


def _safe_indexes():
    return {
        "quiz_sessions": {
            "ttl_terminal_updated_at": {
                "key": [("updated_at_dt", 1)],
                "expireAfterSeconds": 7776000,
                "partialFilterExpression": {
                    "status": {"$in": ["finished", "cancelled"]}
                },
            }
        },
        "miniapp_sessions": {
            "ttl_miniapp_terminal_updated_at": {
                "key": [("updated_at_dt", 1)],
                "expireAfterSeconds": 7776000,
                "partialFilterExpression": {
                    "status": {"$in": ["finished", "abandoned"]}
                },
            }
        },
        "battles": {
            "ttl_battles_delivered_created_at": {
                "key": [("created_at_dt", 1)],
                "expireAfterSeconds": 2592000,
                "partialFilterExpression": {
                    "status": "finalized",
                    "result_delivery.creator.delivered": True,
                    "result_delivery.opponent.delivered": True,
                },
            }
        },
        "reports": {
            "ttl_reports_delivered_created_at": {
                "key": [("created_at_dt", 1)],
                "expireAfterSeconds": 7776000,
                "partialFilterExpression": {"admin_delivered": True},
            }
        },
        "broadcasts": {
            "ttl_broadcast_retention": {
                "key": [("retention_at_dt", 1)],
                "expireAfterSeconds": 7776000,
            }
        },
        "broadcast_deliveries": {
            "ttl_broadcast_delivery_retention": {
                "key": [("retention_at_dt", 1)],
                "expireAfterSeconds": 7776000,
            }
        },
    }


def test_retention_preflight_source_is_read_only():
    source = inspect.getsource(preflight)
    assert "import database" not in source
    for forbidden in (
        ".create_index(",
        ".drop_index(",
        ".insert_one(",
        ".update_one(",
        ".update_many(",
        ".delete_one(",
        ".delete_many(",
        ".find_one_and_update(",
    ):
        assert forbidden not in source


def test_retention_preflight_is_zero_for_exact_state_aware_indexes(monkeypatch, capsys):
    monkeypatch.setattr(preflight, "_load_index_information", _safe_indexes)

    assert preflight.main() == 0
    assert json.loads(capsys.readouterr().out) == {
        "ok": True,
        "retention_indexes": "safe",
    }


def test_missing_new_broadcast_ttls_are_explicit_safe_runtime_bootstrap(monkeypatch, capsys):
    info = _safe_indexes()
    info["broadcasts"] = {}
    info["broadcast_deliveries"] = {"_id_": {"key": [("_id", 1)]}}
    monkeypatch.setattr(preflight, "_load_index_information", lambda: deepcopy(info))

    assert preflight.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["retention_indexes"] == "safe"
    assert payload["bootstrap_pending"] == [
        {
            "collection": "broadcasts",
            "index": "ttl_broadcast_retention",
            "action": "runtime_create_before_http",
        },
        {
            "collection": "broadcast_deliveries",
            "index": "ttl_broadcast_delivery_retention",
            "action": "runtime_create_before_http",
        },
    ]


def test_unrecognized_broadcast_ttl_is_unsafe_not_bootstrap(monkeypatch, capsys):
    info = _safe_indexes()
    info["broadcasts"] = {
        "ttl_unknown": {
            "key": [("created_at_dt", 1)],
            "expireAfterSeconds": 3600,
        }
    }
    monkeypatch.setattr(preflight, "_load_index_information", lambda: deepcopy(info))

    assert preflight.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert any(
        problem["collection"] == "broadcasts"
        and problem["error"] == "unrecognized_ttl_requires_review"
        for problem in payload["problems"]
    )
    assert payload["bootstrap_pending"] == [
        {
            "collection": "broadcasts",
            "index": "ttl_broadcast_retention",
            "action": "runtime_create_before_http",
        }
    ]


def test_retention_preflight_reports_legacy_and_wrong_target(monkeypatch, capsys):
    info = _safe_indexes()
    info["quiz_sessions"]["ttl_updated_at"] = {
        "key": [("updated_at_dt", 1)],
        "expireAfterSeconds": 21600,
    }
    info["miniapp_sessions"]["ttl_miniapp_updated_at"] = {
        "key": [("updated_at_dt", 1)],
        "expireAfterSeconds": 21600,
    }
    info["miniapp_sessions"]["ttl_miniapp_terminal_updated_at"][
        "partialFilterExpression"
    ] = {"status": "finished"}
    info["battles"]["ttl_battles_delivered_created_at"]["key"] = [
        ("updated_at_dt", 1)
    ]
    info["broadcasts"]["ttl_broadcast_created_at"] = {
        "key": [("created_at_dt", 1)],
        "expireAfterSeconds": 7776000,
    }
    info["broadcast_deliveries"]["ttl_broadcast_delivery_retention"][
        "partialFilterExpression"
    ] = {"done": True}
    monkeypatch.setattr(preflight, "_load_index_information", lambda: deepcopy(info))

    assert preflight.main() == 1
    payload = json.loads(capsys.readouterr().out)
    errors = {problem["error"] for problem in payload["problems"]}
    assert "unsafe_legacy_ttl_present" in errors
    assert "state_aware_ttl_wrong_key" in errors
    assert "state_aware_ttl_wrong_filter" in errors
    miniapp_problems = [
        problem
        for problem in payload["problems"]
        if problem["collection"] == "miniapp_sessions"
    ]
    assert {problem["error"] for problem in miniapp_problems} == {
        "unsafe_legacy_ttl_present",
        "state_aware_ttl_wrong_filter",
    }
    broadcast_problems = [
        problem
        for problem in payload["problems"]
        if problem["collection"].startswith("broadcast")
    ]
    assert {problem["error"] for problem in broadcast_problems} == {
        "unsafe_legacy_ttl_present",
        "state_aware_ttl_wrong_filter",
    }


def test_broadcast_retention_preflight_matches_runtime_index_names_and_keys():
    runtime = (ROOT / "broadcast_integrity.py").read_text(encoding="utf-8")
    assert 'name="ttl_broadcast_retention"' in runtime
    assert 'name="ttl_broadcast_delivery_retention"' in runtime
    assert '[("retention_at_dt", ASCENDING)]' in runtime

    specs = {spec[0]: spec for spec in preflight.EXPECTED}
    assert specs["broadcasts"][2:] == (
        "ttl_broadcast_retention",
        [("retention_at_dt", 1)],
        7776000,
        None,
    )
    assert specs["broadcast_deliveries"][2:] == (
        "ttl_broadcast_delivery_retention",
        [("retention_at_dt", 1)],
        7776000,
        None,
    )


def test_retention_preflight_unavailable_is_distinct_failure(monkeypatch, capsys):
    def unavailable():
        raise preflight.RetentionPreflightUnavailable("mongo down")

    monkeypatch.setattr(preflight, "_load_index_information", unavailable)

    assert preflight.main() == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload == {
        "ok": False,
        "error": "preflight_unavailable",
        "detail": "mongo down",
    }


def test_retention_preflight_requires_mongo_url_before_connect(monkeypatch):
    monkeypatch.delenv("MONGO_URL", raising=False)
    monkeypatch.setattr(
        preflight,
        "MongoClient",
        lambda *_args, **_kwargs: pytest.fail("must fail before connecting"),
    )

    with pytest.raises(preflight.RetentionPreflightUnavailable, match="MONGO_URL"):
        preflight._load_index_information()
