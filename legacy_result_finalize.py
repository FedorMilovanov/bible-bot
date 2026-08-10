"""Crash-safe orchestration for legacy Telegram quiz finalization.

This module contains no Telegram UI code. It coordinates the idempotent result
store, achievement policy and owner-scoped session close so handlers can safely
retry after partial Mongo failures without double-crediting users.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from legacy_bonus_store import (
    claim_challenge_bonus_for_result,
    claim_daily_bonus_for_result,
)
from legacy_result_flow import (
    challenge_badge_candidates,
    general_achievement_candidates,
    stable_result_id,
)
from legacy_result_store import (
    LegacyResultStoreUnavailable,
    apply_base_result_once,
    claim_achievement_once,
    result_day,
    result_week_id,
    sync_weekly_best,
)
from session_integrity import QuizSessionStoreUnavailable, finish_owned_quiz_session


class LegacyResultFinalizationPending(RuntimeError):
    """Raised when a durable result stage must be retried later."""


def _award_date(completed_at) -> str:
    try:
        return datetime.fromisoformat(str(completed_at)).strftime("%d.%m.%Y")
    except (TypeError, ValueError):
        return datetime.utcnow().strftime("%d.%m.%Y")


def _claim_achievements(
    *,
    user_id: int,
    keys: list[str],
    rewards: Mapping[str, int],
    awarded_at: str,
) -> list[str]:
    claimed: list[str] = []
    for key in keys:
        reward = max(0, int(rewards.get(key, 0) or 0))
        if claim_achievement_once(
            user_id,
            key,
            reward=reward,
            awarded_at=awarded_at,
        ):
            claimed.append(key)
    return claimed


def _finish_recovery_session(data: dict, user_id: int) -> bool:
    session_id = data.get("session_id")
    if not session_id:
        return False
    finished = finish_owned_quiz_session(str(session_id), user_id)
    # Missing/already-cancelled records are not recoverable, but they must not
    # turn a fully durable score into an infinite retry loop. Mongo failures are
    # surfaced by finish_owned_quiz_session and handled by the outer wrapper.
    return finished is not None


def _durable_result(base: dict) -> dict:
    receipt = base.get("receipt") or {}
    stored = receipt.get("result") if isinstance(receipt, dict) else None
    if isinstance(stored, dict) and stored:
        return stored
    stored = base.get("result")
    return stored if isinstance(stored, dict) else {}


def _achievement_state(base: dict) -> dict:
    receipt = base.get("receipt") or {}
    stored = receipt.get("achievement_state") if isinstance(receipt, dict) else None
    if isinstance(stored, dict) and stored:
        return stored
    user_doc = base.get("user")
    return user_doc if isinstance(user_doc, dict) else {}


def _policy_data(data: dict, durable: dict) -> dict:
    """Use result-time policy inputs on retries instead of mutable handler state."""
    policy = dict(data)
    for key in ("quiz_mode", "fastest_answer"):
        if key in durable:
            policy[key] = durable[key]
    return policy


def finalize_normal_result(
    *,
    user_id: int,
    data: dict,
    score: int,
    total: int,
    time_seconds: float,
    achievement_rewards: Mapping[str, int],
) -> dict:
    """Durably finalize one ordinary legacy quiz result.

    Retry-error drills intentionally remain unscored, matching legacy product
    semantics and preventing bonus/achievement farming through the review flow.
    """
    if data.get("is_retry"):
        return {
            "scored": False,
            "earned_base": 0,
            "daily_bonus": {"bonus": 0, "eligible": False, "claimed_now": False},
            "new_achievements": [],
            "session_finished": False,
        }

    try:
        result_id = stable_result_id(user_id, data)
        base = apply_base_result_once(
            result_id=result_id,
            user_id=user_id,
            username=data.get("username") or "",
            first_name=data.get("first_name") or "Игрок",
            level_key=data["level_key"],
            score=score,
            total=total,
            time_seconds=time_seconds,
            score_multiplier=data.get("score_multiplier", 1.0),
            max_streak=data.get("max_streak", 0),
            quiz_mode=data.get("quiz_mode"),
            fastest_answer=data.get("fastest_answer"),
        )
        completed_at = base["completed_at"]
        receipt = base.get("receipt") or {}
        durable = _durable_result(base)
        achievement_state = _achievement_state(base)
        day = result_day(completed_at)
        daily_bonus = claim_daily_bonus_for_result(
            user_id=user_id,
            result_id=result_id,
            day=day,
            daily_streak=int(receipt.get("daily_streak", 0) or 0),
        )
        keys = general_achievement_candidates(
            achievement_state,
            _policy_data(data, durable),
        )
        claimed = _claim_achievements(
            user_id=user_id,
            keys=keys,
            rewards=achievement_rewards,
            awarded_at=_award_date(completed_at),
        )
        session_finished = _finish_recovery_session(data, user_id)
        return {
            "scored": True,
            "result_id": result_id,
            "base_applied_now": bool(base["applied"]),
            "earned_base": int(base["earned_base"]),
            "completed_at": completed_at,
            "daily_bonus": daily_bonus,
            "new_achievements": claimed,
            "session_finished": session_finished,
        }
    except (LegacyResultStoreUnavailable, QuizSessionStoreUnavailable) as exc:
        raise LegacyResultFinalizationPending("normal result finalization is retryable") from exc


def finalize_challenge_result(
    *,
    user_id: int,
    data: dict,
    score: int,
    total: int,
    time_seconds: float,
    achievement_rewards: Mapping[str, int],
) -> dict:
    """Durably finalize one Challenge result with retry-stable bonus/week data."""
    requested_mode = str(data["challenge_mode"])
    try:
        result_id = stable_result_id(user_id, data)
        base = apply_base_result_once(
            result_id=result_id,
            user_id=user_id,
            username=data.get("username") or "",
            first_name=data.get("first_name") or "Игрок",
            level_key=requested_mode,
            score=score,
            total=total,
            time_seconds=time_seconds,
            score_multiplier=1.0,
            max_streak=data.get("max_streak", 0),
            challenge_mode=requested_mode,
            quiz_mode=data.get("quiz_mode"),
            fastest_answer=data.get("fastest_answer"),
        )
        completed_at = base["completed_at"]
        durable = _durable_result(base)
        achievement_state = _achievement_state(base)
        mode = str(durable.get("challenge_mode") or requested_mode)
        durable_score = int(durable.get("score", score) or 0)
        durable_time = max(0.0, float(durable.get("time_seconds", time_seconds) or 0.0))
        day = result_day(completed_at)

        bonus = claim_challenge_bonus_for_result(
            user_id=user_id,
            result_id=result_id,
            mode=mode,
            score=durable_score,
            day=day,
        )

        # Weekly ranking is independent from the once-per-day bonus. Every
        # attempt may improve the weekly best, including second attempts.
        sync_weekly_best(
            user_id=user_id,
            username=data.get("username") or "",
            first_name=data.get("first_name") or "Игрок",
            mode=mode,
            score=durable_score,
            time_seconds=durable_time,
            week_id=result_week_id(completed_at),
        )

        general_keys = general_achievement_candidates(
            achievement_state,
            _policy_data(data, durable),
        )
        general_claimed = _claim_achievements(
            user_id=user_id,
            keys=general_keys,
            rewards=achievement_rewards,
            awarded_at=_award_date(completed_at),
        )

        # Challenge policy intentionally returns (storage key, legacy UI text)
        # pairs. Persist only the canonical key, then surface the message for
        # claims that actually won the idempotent achievement update.
        badge_candidates = challenge_badge_candidates(achievement_state, durable_score)
        badge_keys = [key for key, _message in badge_candidates]
        badge_claimed_keys = _claim_achievements(
            user_id=user_id,
            keys=badge_keys,
            rewards={},
            awarded_at=_award_date(completed_at),
        )
        claimed_key_set = set(badge_claimed_keys)
        badge_messages = [
            message for key, message in badge_candidates if key in claimed_key_set
        ]

        session_finished = _finish_recovery_session(data, user_id)
        return {
            "scored": True,
            "result_id": result_id,
            "base_applied_now": bool(base["applied"]),
            "earned_base": int(base["earned_base"]),
            "completed_at": completed_at,
            "bonus": bonus,
            "new_achievements": general_claimed,
            "new_challenge_badges": badge_messages,
            "session_finished": session_finished,
        }
    except (LegacyResultStoreUnavailable, QuizSessionStoreUnavailable) as exc:
        raise LegacyResultFinalizationPending("challenge result finalization is retryable") from exc
