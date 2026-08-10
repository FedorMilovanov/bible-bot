from pathlib import Path

path = Path("tests/test_legacy_result_store.py")
text = path.read_text(encoding="utf-8")
old = '''def test_daily_bonus_public_api_preserves_replay_state(monkeypatch):
    doc = base_user()
    doc["daily_activity_streak"] = 3
    users = FakeUsers(doc)
    monkeypatch.setattr(database, "collection", users)

    first = store.claim_daily_bonus_once(42, "2026-08-10")
    replay = store.claim_daily_bonus_once(42, "2026-08-10")

    assert first == {"bonus": 10, "eligible": True, "claimed_now": True}
    assert replay == {"bonus": 10, "eligible": True, "claimed_now": False}
    assert users.doc["total_points"] == 10
'''
new = '''def test_daily_bonus_compatibility_wrapper_returns_only_new_credit(monkeypatch):
    doc = base_user()
    doc["daily_activity_streak"] = 3
    users = FakeUsers(doc)
    monkeypatch.setattr(database, "collection", users)

    assert store.claim_daily_bonus_once(42, "2026-08-10") == 10
    assert store.claim_daily_bonus_once(42, "2026-08-10") == 0
    assert users.doc["total_points"] == 10
'''
if text.count(old) != 1:
    raise SystemExit(f"expected one contradictory test block, got {text.count(old)}")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
