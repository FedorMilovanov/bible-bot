from pathlib import Path

path = Path("legacy_result_store.py")
text = path.read_text(encoding="utf-8")

old_daily = '''def claim_daily_bonus_once(user_id: int, day: str, daily_streak: int | None = None) -> int:
    """Compatibility wrapper: return points only for the winning claim."""
    if daily_streak is None:
        entry = _users().find_one(
            {"_id": database._uid(user_id)}, {"daily_activity_streak": 1}
        ) or {}
        daily_streak = int(entry.get("daily_activity_streak", 0) or 0)
    stage = claim_daily_bonus_state(user_id, day, daily_streak)
    return stage["bonus"] if stage["claimed_now"] else 0
'''
new_daily = '''def claim_daily_bonus_once(user_id: int, day: str, daily_streak: int | None = None) -> dict:
    """Return the durable daily-bonus stage, including replay metadata."""
    if daily_streak is None:
        entry = _users().find_one(
            {"_id": database._uid(user_id)}, {"daily_activity_streak": 1}
        ) or {}
        daily_streak = int(entry.get("daily_activity_streak", 0) or 0)
    return claim_daily_bonus_state(user_id, day, daily_streak)
'''
old_challenge = '''def claim_challenge_bonus_once(user_id: int, mode: str, score: int, day: str) -> int:
    """Compatibility wrapper: return points only for the winning claim."""
    stage = claim_challenge_bonus_state(user_id, mode, score, day)
    return stage["bonus"] if stage["claimed_now"] else 0
'''
new_challenge = '''def claim_challenge_bonus_once(user_id: int, mode: str, score: int, day: str) -> dict:
    """Return the durable Challenge-bonus stage, including replay metadata."""
    return claim_challenge_bonus_state(user_id, mode, score, day)
'''

for label, old, new in (
    ("daily wrapper", old_daily, new_daily),
    ("challenge wrapper", old_challenge, new_challenge),
):
    if text.count(old) != 1:
        raise SystemExit(f"{label}: expected one exact block, got {text.count(old)}")
    text = text.replace(old, new, 1)

if 'return stage["bonus"] if stage["claimed_now"] else 0' in text:
    raise SystemExit("legacy int wrapper remains")
path.write_text(text, encoding="utf-8")
