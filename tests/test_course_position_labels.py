from questions import POOL_REGISTRY


def test_project_position_is_visibly_labeled_in_intro():
    for key in ("intro1", "intro2"):
        for item in POOL_REGISTRY[key]:
            if item["position"] == "project":
                assert item["question"].startswith("[Позиция курса]")


def test_final_authorship_question_names_the_course_position():
    item = next(q for q in POOL_REGISTRY["intro3"] if q["id"] == "intro3_16")
    assert item["position"] == "project"
    assert "курс" in item["question"].casefold()
    assert item["competitive"] is False
