import pytest

from legacy_callback_protocol import (
    build_answer_callback,
    callback_matches_option,
    callback_matches_session,
    option_callback_token,
    parse_answer_callback,
    session_callback_token,
)


def test_callback_round_trip_binds_session_question_slot_and_option_text():
    payload = build_answer_callback("qa", "session-123", 7, 2, "Rendered answer")

    token, question_index, option_index, option_token = parse_answer_callback(payload, "qa")

    assert callback_matches_session(token, "session-123") is True
    assert callback_matches_session(token, "session-other") is False
    assert callback_matches_option(option_token, "Rendered answer") is True
    assert callback_matches_option(option_token, "Different answer") is False
    assert question_index == 7
    assert option_index == 2
    assert len(payload.encode("utf-8")) <= 64


def test_challenge_and_normal_prefixes_are_not_interchangeable():
    payload = build_answer_callback("cha", "session-123", 0, 1, "A")

    with pytest.raises(ValueError, match="malformed answer callback"):
        parse_answer_callback(payload, "qa")


def test_legacy_option_only_callback_is_rejected_as_ambiguous():
    with pytest.raises(ValueError, match="malformed answer callback"):
        parse_answer_callback("qa_2", "qa")


def test_old_four_part_callback_is_rejected_without_option_identity():
    token = session_callback_token("session-123")
    with pytest.raises(ValueError, match="malformed answer callback"):
        parse_answer_callback(f"qa:{token}:0:1", "qa")


def test_question_index_change_changes_payload_even_for_same_session_slot_and_text():
    first = build_answer_callback("qa", "session-123", 0, 1, "A")
    second = build_answer_callback("qa", "session-123", 1, 1, "A")

    assert first != second
    assert parse_answer_callback(first, "qa")[1:3] == (0, 1)
    assert parse_answer_callback(second, "qa")[1:3] == (1, 1)


def test_option_text_change_changes_option_fingerprint_for_same_slot():
    first = build_answer_callback("qa", "session-123", 0, 1, "A")
    second = build_answer_callback("qa", "session-123", 0, 1, "B")

    assert first != second
    first_option_token = parse_answer_callback(first, "qa")[3]
    second_option_token = parse_answer_callback(second, "qa")[3]
    assert first_option_token != second_option_token


def test_tokens_are_compact_deterministic_and_nonempty():
    session_first = session_callback_token("quiz-session-id")
    session_second = session_callback_token("quiz-session-id")
    option_first = option_callback_token("Answer")
    option_second = option_callback_token("Answer")

    assert session_first == session_second
    assert len(session_first) == 12
    assert option_first == option_second
    assert len(option_first) == 12


def test_protocol_rejects_invalid_indexes_and_tokens():
    with pytest.raises(ValueError, match="question_index"):
        build_answer_callback("qa", "s1", -1, 0, "A")
    with pytest.raises(ValueError, match="option_index"):
        build_answer_callback("qa", "s1", 0, True, "A")
    with pytest.raises(ValueError, match="option_text"):
        build_answer_callback("qa", "s1", 0, 0, None)
    with pytest.raises(ValueError, match="malformed session callback token"):
        parse_answer_callback("qa:not-a-token:0:1:abcdefghijkl", "qa")
    token = session_callback_token("s1")
    with pytest.raises(ValueError, match="malformed option callback token"):
        parse_answer_callback(f"qa:{token}:0:1:bad", "qa")


def test_protocol_rejects_missing_session_id_and_unknown_prefix():
    with pytest.raises(ValueError, match="session_id"):
        build_answer_callback("qa", "", 0, 0, "A")
    with pytest.raises(ValueError, match="unsupported callback prefix"):
        build_answer_callback("other", "s1", 0, 0, "A")
