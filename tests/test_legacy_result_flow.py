import legacy_result_flow as flow


def test_stable_result_id_prefers_persisted_session_and_is_memoized():
    data = {"session_id": "abc-123", "start_time": 10, "level_key": "easy", "questions": []}

    first = flow.stable_result_id(42, data)
    data["session_id"] = "changed"
    second = flow.stable_result_id(42, data)

    assert first == "quiz:abc-123"
    assert second == first


def test_stable_result_id_has_deterministic_memory_fallback():
    first_data = {
        "session_id": None,
        "start_time": 123.5,
        "level_key": "random_all",
        "questions": [{"id": "q1"}, {"id": "q2"}],
    }
    second_data = {
        "session_id": None,
        "start_time": 123.5,
        "level_key": "random_all",
        "questions": [{"id": "q1"}, {"id": "q2"}],
    }

    assert flow.stable_result_id(42, first_data) == flow.stable_result_id(42, second_data)
    assert first_data["result_id"].startswith("memory:")


def test_general_achievement_candidates_use_durable_post_result_counters():
    user_doc = {
        "total_tests": 10,
        "perfect_count": 5,
        "max_streak_ever": 10,
        "daily_activity_streak": 7,
    }
    data = {"quiz_mode": "speed", "fastest_answer": 2.9}

    candidates = flow.general_achievement_candidates(user_doc, data)

    assert candidates == [
        "first_steps",
        "perfectionist_1",
        "perfectionist_2",
        "streak_5",
        "streak_10",
        "marathoner_10",
        "daily_streak_7",
        "lightning",
    ]


def test_general_achievement_candidates_do_not_invent_thresholds():
    assert flow.general_achievement_candidates({}, {}) == []


def test_challenge_badges_preserve_legacy_semantics():
    assert flow.challenge_badge_candidates({"challenge_streak_count": 3}, 20) == [
        ("streak_3", "🔥 3-дневная серия 18+ — разблокировано!"),
        ("perfect_20", "⭐ Perfect 20 — разблокировано!"),
    ]
    assert flow.challenge_badge_candidates({"challenge_streak_count": 2}, 19) == []
