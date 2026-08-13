import pytest

from legacy_session_callback_protocol import (
    build_session_action_callback,
    callback_matches_attempt,
    parse_session_action_callback,
)

_SESSION = "12345678-1234-5678-9234-567812345678"


def test_lifecycle_callback_fits_telegram_limit_and_round_trips():
    for action in ("res", "rst", "can"):
        payload = build_session_action_callback(action, _SESSION, "attempt-1")
        assert len(payload.encode("utf-8")) <= 64
        session_id, token = parse_session_action_callback(payload, action)
        assert session_id == _SESSION
        assert callback_matches_attempt(token, "attempt-1") is True


def test_same_container_old_button_expires_after_restart():
    payload = build_session_action_callback("rst", _SESSION, "attempt-old")
    session_id, token = parse_session_action_callback(payload, "rst")

    assert session_id == _SESSION
    assert callback_matches_attempt(token, "attempt-old") is True
    assert callback_matches_attempt(token, "attempt-new") is False


def test_wrong_action_or_malformed_payload_fails_closed():
    payload = build_session_action_callback("res", _SESSION, "attempt-1")
    with pytest.raises(ValueError, match="malformed"):
        parse_session_action_callback(payload, "can")
    with pytest.raises(ValueError, match="session_id"):
        build_session_action_callback("res", "not-a-uuid", "attempt-1")
    with pytest.raises(ValueError, match="unsupported"):
        build_session_action_callback("other", _SESSION, "attempt-1")


def test_attempt_token_comparison_rejects_invalid_values():
    assert callback_matches_attempt("bad", "attempt-1") is False
    assert callback_matches_attempt(None, "attempt-1") is False
