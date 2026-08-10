"""Pure policy helpers for crash-safe legacy quiz finalization."""
from __future__ import annotations

import uuid


_GENERAL_THRESHOLDS = (
    ("perfectionist_1", "perfect_count", 1),
    ("perfectionist_2", "perfect_count", 5),
    ("perfectionist_3", "perfect_count", 15),
    ("streak_5", "max_streak_ever", 5),
    ("streak_10", "max_streak_ever", 10),
    ("streak_20", "max_streak_ever", 20),
    ("marathoner_10", "total_tests", 10),
    ("marathoner_50", "total_tests", 50),
    ("marathoner_100", "total_tests", 100),
    ("daily_streak_7", "daily_activity_streak", 7),
    ("daily_streak_30", "daily_activity_streak", 30),
)


def stable_result_id(user_id: int, data: dict) -> str:
    """Return and memoize an idempotency key for one quiz result.

    Persisted quizzes use their Mongo session id. If session persistence was
    unavailable at start, recovery across a process crash is impossible anyway;
    a fresh UUID is therefore safer than a deterministic hash of incomplete
    runtime fields, which could collapse two distinct attempts into one receipt.
    Repeated finalization of the same in-memory ``data`` object remains stable
    because the generated id is memoized immediately.
    """
    del user_id  # kept in the public signature for call-site clarity/backward compatibility

    existing = str(data.get("result_id") or "").strip()
    if existing:
        return existing

    session_id = str(data.get("session_id") or "").strip()
    if session_id:
        result_id = f"quiz:{session_id}"
    else:
        result_id = f"memory:{uuid.uuid4().hex}"

    data["result_id"] = result_id
    return result_id


def general_achievement_candidates(user_doc: dict, data: dict) -> list[str]:
    """Return general achievement keys satisfied by the durable post-result state."""
    candidates: list[str] = []
    if int(user_doc.get("total_tests", 0) or 0) >= 1:
        candidates.append("first_steps")

    for key, field, threshold in _GENERAL_THRESHOLDS:
        if int(user_doc.get(field, 0) or 0) >= threshold:
            candidates.append(key)

    if (
        data.get("quiz_mode") == "speed"
        and float(data.get("fastest_answer", 9999) or 9999) <= 3
    ):
        candidates.append("lightning")
    return candidates


def challenge_badge_candidates(user_doc: dict, score: int) -> list[tuple[str, str]]:
    """Return the two legacy Challenge badges that carry no point reward."""
    result: list[tuple[str, str]] = []
    if int(user_doc.get("challenge_streak_count", 0) or 0) >= 3:
        result.append(("streak_3", "🔥 3-дневная серия 18+ — разблокировано!"))
    if int(score) == 20:
        result.append(("perfect_20", "⭐ Perfect 20 — разблокировано!"))
    return result
