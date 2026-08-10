import pytest

from legacy_callback_protocol import (
    build_answer_callback,
    callback_matches_session,
    parse_answer_callback,
    session_callback_token,
)


def test_callback_round_trip_binds_session_question_and_option():
    payload = build_answer_callback("qa", "session-123", 7, 2)

    token, question_index, option_index = parse_answer_callback(payload, "qa")

    assert callback_matches_session(token, "session-123") is True
    assert callback_matches_session(token, "session-other") is False
    assert question_index == 7
    assert option_index == 2
    assert len(payload.encode("utf-8")) <= 64


def test_challenge_and_normal_prefixes_are_not_interchangeable():
    payload = build_answer_callback("cha", "session-123", 0, 1)

    with pytest.raises(ValueError, match="malformed answer callback"):
        parse_answer_callback(payload, "qa")


def test_legacy_option_only_callback_is_rejected_as_ambiguous():
    with pytest.raises(ValueError, match="malformed answer callback"):
        parse_answer_callback("qa_2", "qa")


def test_question_index_change_changes_payload_even_for_same_session_and_option():
    first = build_answer_callback("qa", "session-123", 0, 1)
    second = build_answer_callback("qa", "session-123", 1, 1)

    assert first != second
    assert parse_answer_callback(first, "qa")[1:] == (0, 1)
    assert parse_answer_callback(second, "qa")[1:] == (1, 1)


def test_session_token_is_compact_deterministic_and_nonempty():
    first = session_callback_token("quiz-session-id")
    second = session_callback_token("quiz-session-id")

    assert first == second
    assert len(first) == 12


def test_protocol_rejects_invalid_indexes_and_tokens():
    with pytest.raises(ValueError, match="question_index"):
        build_answer_callback("qa", "s1", -1, 0)
    with pytest.raises(ValueError, match="option_index"):
        build_answer_callback("qa", "s1", 0, True)
    with pytest.raises(ValueError, match="malformed session callback token"):
        parse_answer_callback("qa:not-a-token:0:1", "qa")


def test_protocol_rejects_missing_session_id_and_unknown_prefix():
    with pytest.raises(ValueError, match="session_id"):
        build_answer_callback("qa", "", 0, 0)
    with pytest.raises(ValueError, match="unsupported callback prefix"):
        build_answer_callback("other", "s1", 0, 0)
