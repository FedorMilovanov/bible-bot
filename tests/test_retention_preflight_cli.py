import json
from copy import deepcopy

import pytest

import scripts.check_retention_indexes as preflight


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
    }


def test_retention_preflight_is_zero_for_exact_state_aware_indexes(monkeypatch, capsys):
    monkeypatch.setattr(preflight, "_load_index_information", _safe_indexes)

    assert preflight.main() == 0
    assert json.loads(capsys.readouterr().out) == {
        "ok": True,
        "retention_indexes": "safe",
    }


def test_retention_preflight_reports_legacy_and_wrong_target(monkeypatch, capsys):
    info = _safe_indexes()
    info["quiz_sessions"]["ttl_updated_at"] = {
        "key": [("updated_at_dt", 1)],
        "expireAfterSeconds": 21600,
    }
    info["battles"]["ttl_battles_delivered_created_at"]["key"] = [
        ("updated_at_dt", 1)
    ]
    monkeypatch.setattr(preflight, "_load_index_information", lambda: deepcopy(info))

    assert preflight.main() == 1
    payload = json.loads(capsys.readouterr().out)
    errors = {problem["error"] for problem in payload["problems"]}
    assert "unsafe_legacy_ttl_present" in errors
    assert "state_aware_ttl_wrong_key" in errors


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
