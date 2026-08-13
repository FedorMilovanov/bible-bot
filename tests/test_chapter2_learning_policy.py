from questions.pool_policy import is_non_scoring_learning_pool


def test_chapter2_uses_learning_only_policy():
    assert is_non_scoring_learning_pool("chapter2") is True
    assert is_non_scoring_learning_pool("easy_p1") is False
    assert is_non_scoring_learning_pool("random20") is False
