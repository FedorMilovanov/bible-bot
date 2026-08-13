from questions import POOL_REGISTRY


def _item(qid):
    for pool in POOL_REGISTRY.values():
        for question in pool:
            if question.get("id") == qid:
                return question
    raise AssertionError(qid)


def test_second_pass_greek_corrections_are_live():
    assert "\u043e\u0434\u043d\u0443 \u043a\u043e\u043d\u043a\u0440\u0435\u0442\u043d\u0443\u044e \u0441\u0438\u0441\u0442\u0435\u043c\u0443" in _item("ling1_01")["options"][0]
    item_108 = _item("ling1_08")
    assert item_108["options"][item_108["correct"]] == "\u0420\u0430\u0441\u0441\u0435\u044f\u043d\u0438\u0435 / \u0434\u0438\u0430\u0441\u043f\u043e\u0440\u0430"
    assert "\u1f10\u03c3\u03c4\u03af\u03bd" not in _item("ling2_02")["question"]
    assert "\u0447\u0430\u0441\u0442\u043e \u043f\u0440\u0435\u0434\u043b\u0430\u0433\u0430\u044e\u0442" in _item("ling2_06")["question"]
    assert "\u0441\u0438\u043d\u0442\u0435\u0437\u0430 \u043d\u043e\u0432\u043e\u0437\u0430\u0432\u0435\u0442\u043d\u044b\u0445 \u0442\u0435\u043a\u0441\u0442\u043e\u0432" in _item("ling2_13")["explanation"]
    assert "\u044d\u0442\u0438\u043c\u043e\u043b\u043e\u0433" in _item("ling3_15")["explanation"]


def test_old_greek_corruption_markers_do_not_reach_questions_or_options():
    text = "\n".join(
        str(value)
        for key in ("linguistics_ch1", "linguistics_ch1_2", "linguistics_ch1_3")
        for item in POOL_REGISTRY[key]
        for value in (item["question"], *item["options"])
    )
    forbidden = (
        "[\u0441\u043f\u0438\u043b\u044e",
        "\u03b4\u03b9\u03b1\u03c3\u03c0\u03bf\u03c1\u03ac [\u0441\u043f\u043e\u0440\u0430",
        "\u0410\u043e\u0440\u0438\u0441\u0442 = \u0444\u0430\u043a\u0442/\u0438\u0442\u043e\u0433",
        "\u1f00\u03b3\u03b1\u03c0\u1fb6\u03c4\u03b5",
    )
    for fragment in forbidden:
        assert fragment not in text
