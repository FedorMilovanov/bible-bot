import pytest

from legacy_battle_callback_protocol import (
    LegacyBattleCallbackInvalid,
    battle_callback_token,
    battle_option_token,
    build_battle_answer_callback,
    callback_matches_battle,
    parse_battle_answer_callback,
    resolve_battle_option,
)


def test_battle_callback_round_trip_and_telegram_limit():
    payload = build_battle_answer_callback("battle_42_1234567890", 7, "Апостол Пётр")

    battle_token, question_index, option_token = parse_battle_answer_callback(payload)

    assert question_index == 7
    assert battle_token == battle_callback_token("battle_42_1234567890")
    assert option_token == battle_option_token("Апостол Пётр")
    assert callback_matches_battle("battle_42_1234567890", battle_token) is True
    assert len(payload.encode()) <= 64


def test_old_display_index_protocol_is_rejected():
    with pytest.raises(LegacyBattleCallbackInvalid):
        parse_battle_answer_callback("ba_2")


def test_callback_token_is_battle_specific():
    payload = build_battle_answer_callback("battle-a", 0, "A")
    token, _, _ = parse_battle_answer_callback(payload)

    assert callback_matches_battle("battle-a", token) is True
    assert callback_matches_battle("battle-b", token) is False


def test_semantic_option_survives_display_reshuffle():
    payload = build_battle_answer_callback("battle-a", 0, "Пётр")
    _, _, option_token = parse_battle_answer_callback(payload)

    assert resolve_battle_option(["Павел", "Пётр", "Иоанн"], option_token) == "Пётр"
    assert resolve_battle_option(["Иоанн", "Павел", "Пётр"], option_token) == "Пётр"


def test_duplicate_semantic_option_is_ambiguous():
    token = battle_option_token("Пётр")

    with pytest.raises(LegacyBattleCallbackInvalid, match="stale or ambiguous"):
        resolve_battle_option(["Пётр", "Пётр"], token)


@pytest.mark.parametrize("question_index", [-1, True, 1.5, "1"])
def test_builder_rejects_invalid_question_index(question_index):
    with pytest.raises(ValueError):
        build_battle_answer_callback("battle-a", question_index, "A")


@pytest.mark.parametrize(
    "payload",
    [
        "",
        "bq:short:0:short",
        "bq:aaaaaaaaaaaa:-1:bbbbbbbbbbbb",
        "bq:aaaaaaaaaaaa:0:bad!token!!!",
        "qa:aaaaaaaaaaaa:0:bbbbbbbbbbbb",
    ],
)
def test_parser_rejects_malformed_payload(payload):
    with pytest.raises(LegacyBattleCallbackInvalid):
        parse_battle_answer_callback(payload)
