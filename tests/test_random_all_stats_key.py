import database


def test_random_all_is_a_first_class_stats_key():
    assert database._safe_level_key("random_all") == "random_all"
    assert "random_all" in database.ALL_LEVEL_KEYS
    assert database.POINTS_PER_QUESTION["random_all"] == 1


def test_unknown_level_still_falls_back_to_easy():
    assert database._safe_level_key("definitely-not-a-level") == "easy"
