"""Compact stale-UI protocol for durable PvP answer callbacks."""
from __future__ import annotations

import base64
import hashlib
import hmac
import re

_BATTLE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{12}$")
_OPTION_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{12}$")
_PREFIX = "bq"


class LegacyBattleCallbackInvalid(ValueError):
    """Raised when a PvP callback payload is malformed."""


def _fingerprint(value: str) -> str:
    digest = hashlib.sha256(value.encode()).digest()[:9]
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


def battle_callback_token(battle_id: str) -> str:
    if not isinstance(battle_id, str) or not battle_id.strip():
        raise ValueError("battle_id is required")
    return _fingerprint(battle_id.strip())


def battle_option_token(option_text: str) -> str:
    if not isinstance(option_text, str) or not option_text:
        raise ValueError("option_text is required")
    return _fingerprint(option_text)


def build_battle_answer_callback(
    battle_id: str,
    question_index: int,
    option_text: str,
) -> str:
    if isinstance(question_index, bool) or not isinstance(question_index, int) or question_index < 0:
        raise ValueError("question_index must be a non-negative integer")
    payload = (
        f"{_PREFIX}:{battle_callback_token(battle_id)}:"
        f"{question_index}:{battle_option_token(option_text)}"
    )
    if len(payload.encode()) > 64:
        raise ValueError("battle callback payload exceeds Telegram's 64-byte limit")
    return payload


def parse_battle_answer_callback(payload: str) -> tuple[str, int, str]:
    if not isinstance(payload, str):
        raise LegacyBattleCallbackInvalid("battle callback must be a string")
    parts = payload.split(":")
    if len(parts) != 4 or parts[0] != _PREFIX:
        raise LegacyBattleCallbackInvalid("unsupported battle callback format")
    battle_token, raw_index, option_token = parts[1:]
    if not _BATTLE_TOKEN_RE.fullmatch(battle_token):
        raise LegacyBattleCallbackInvalid("invalid battle callback token")
    if not _OPTION_TOKEN_RE.fullmatch(option_token):
        raise LegacyBattleCallbackInvalid("invalid battle option token")
    if not raw_index.isdigit():
        raise LegacyBattleCallbackInvalid("invalid battle question index")
    question_index = int(raw_index)
    return battle_token, question_index, option_token


def callback_matches_battle(battle_id: str, callback_token: str) -> bool:
    if not isinstance(callback_token, str) or not _BATTLE_TOKEN_RE.fullmatch(callback_token):
        return False
    return hmac.compare_digest(battle_callback_token(battle_id), callback_token)


def resolve_battle_option(options: list[str], option_token: str) -> str:
    if not isinstance(options, list) or not options:
        raise LegacyBattleCallbackInvalid("battle options are invalid")
    if not isinstance(option_token, str) or not _OPTION_TOKEN_RE.fullmatch(option_token):
        raise LegacyBattleCallbackInvalid("invalid battle option token")
    matches = [
        option
        for option in options
        if isinstance(option, str)
        and option
        and hmac.compare_digest(battle_option_token(option), option_token)
    ]
    if len(matches) != 1:
        raise LegacyBattleCallbackInvalid("battle option token is stale or ambiguous")
    return matches[0]
