import pytest

from legacy_attempt_identity import (
    bind_runtime_attempt,
    persisted_attempt_id,
    runtime_attempt_id,
)


def test_persisted_attempt_prefers_explicit_attempt_id():
    assert persisted_attempt_id({"_id": "container", "attempt_id": "attempt-2"}) == "attempt-2"


def test_legacy_session_falls_back_to_container_id():
    assert persisted_attempt_id({"_id": "legacy-session"}) == "legacy-session"


def test_runtime_attempt_falls_back_to_session_id_for_legacy_data():
    assert runtime_attempt_id({"session_id": "legacy-session"}) == "legacy-session"
    assert runtime_attempt_id({}) is None


def test_bind_runtime_attempt_uses_durable_attempt():
    data = {"session_id": "container"}
    assert bind_runtime_attempt(data, {"_id": "container", "attempt_id": "attempt-2"}) == "attempt-2"
    assert data["attempt_id"] == "attempt-2"


def test_malformed_attempt_identity_fails_closed():
    for value in ("", "   ", 7, True):
        with pytest.raises(ValueError, match="attempt_id"):
            persisted_attempt_id({"_id": "container", "attempt_id": value})
