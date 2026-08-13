import pytest

import legacy_session_launch as launch
from legacy_retry_policy import LegacyRetryPolicyInvalid, persisted_is_retry


def test_persisted_retry_policy_defaults_false_for_legacy_sessions():
    assert persisted_is_retry({"mode": "level"}) is False


def test_persisted_retry_policy_accepts_level_practice_only():
    assert persisted_is_retry({"mode": "level", "is_retry": True}) is True
    for session in (
        {"mode": "level", "is_retry": "true"},
        {"mode": "random20", "is_retry": True},
        {"mode": "hardcore20", "is_retry": True},
    ):
        with pytest.raises(LegacyRetryPolicyInvalid):
            persisted_is_retry(session)


def test_launch_propagates_retry_policy_to_strict_create(monkeypatch):
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: None)
    captured = {}

    def create(**kwargs):
        captured.update(kwargs)
        return {
            "_id": "practice-session",
            "attempt_id": "practice-session",
            "status": "in_progress",
            "mode": "level",
            "is_retry": kwargs["is_retry"],
        }

    monkeypatch.setattr(launch, "create_quiz_session_strict", create)
    outcome = launch.launch_quiz_attempt(
        user_id=42,
        mode="level",
        question_ids=["q1"],
        questions_data=[{"id": "q1"}],
        level_key="easy",
        level_name="Retry errors",
        time_limit=None,
        chat_id=100,
        is_retry=True,
    )

    assert outcome.session_id == "practice-session"
    assert captured["is_retry"] is True
