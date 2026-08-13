from types import SimpleNamespace

import pytest

import legacy_session_control as control


def _active():
    return {
        "_id": "container-1",
        "attempt_id": "attempt-1",
        "status": "in_progress",
    }


def _install_active(monkeypatch):
    monkeypatch.setattr(control, "get_active_quiz_session_strict", lambda _user_id: _active())
    monkeypatch.setattr(
        control,
        "classify_restart_session",
        lambda _session: SimpleNamespace(action="resume"),
    )


def test_valid_idempotent_cancel_snapshot_is_accepted(monkeypatch):
    _install_active(monkeypatch)
    monkeypatch.setattr(
        control,
        "cancel_owned_incomplete_quiz_attempt",
        lambda *_args, **_kwargs: {
            "applied": False,
            "session": {
                "_id": "container-1",
                "attempt_id": "attempt-1",
                "status": "cancelled",
            },
        },
    )

    result = control.cancel_current_incomplete_session(42)

    assert result.had_active_session is True
    assert result.cancelled_now is False
    assert result.session_id == "container-1"
    assert result.attempt_id == "attempt-1"


@pytest.mark.parametrize(
    "cancel_result, message",
    [
        (
            {
                "applied": "yes",
                "session": {
                    "_id": "container-1",
                    "attempt_id": "attempt-1",
                    "status": "cancelled",
                },
            },
            "applied state",
        ),
        (
            {
                "applied": True,
                "session": {
                    "_id": "container-other",
                    "attempt_id": "attempt-1",
                    "status": "cancelled",
                },
            },
            "durable session",
        ),
        (
            {
                "applied": True,
                "session": {
                    "_id": "container-1",
                    "attempt_id": "attempt-other",
                    "status": "cancelled",
                },
            },
            "another quiz attempt",
        ),
        (
            {
                "applied": True,
                "session": {
                    "_id": "container-1",
                    "attempt_id": "attempt-1",
                    "status": "in_progress",
                },
            },
            "cancelled durable state",
        ),
    ],
)
def test_malformed_cancel_postcondition_fails_closed(monkeypatch, cancel_result, message):
    _install_active(monkeypatch)
    monkeypatch.setattr(
        control,
        "cancel_owned_incomplete_quiz_attempt",
        lambda *_args, **_kwargs: cancel_result,
    )

    with pytest.raises(control.LegacySessionControlConflict, match=message):
        control.cancel_current_incomplete_session(42)
