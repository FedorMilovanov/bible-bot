from questions import POOL_REGISTRY


def _item(qid):
    for pool in POOL_REGISTRY.values():
        for question in pool:
            if question.get("id") == qid:
                return question
    raise AssertionError(qid)


def test_second_pass_greek_corrections_are_live():
    assert "одну конкретную систему" in _item("ling1_01")["options"][0]
    assert _item("ling1_08")["options"][_item("ling1_08")["correct"]] == "Рассеяние / диаспора"
    assert "ἐστίν" not in _item("ling2_02")["question"]
    assert "часто предлагают" in _item("ling2_06")["question"]
    assert "богословски" in _item("ling2_13")["explanation"]
    assert "этимолог" in _item("ling3_15")["explanation"]


def test_old_greek_corruption_markers_do_not_reach_canonical_runtime():
    text = "\n".join(
        str(value)
        for key in ("linguistics_ch1", "linguistics_ch1_2", "linguistics_ch1_3")
        for item in POOL_REGISTRY[key]
        for value in (item["question"], item["explanation"], *item["options"])
    )
    for fragment in ("[спилю", "διασπορά [спора", "Аорист = факт/итог", "ἀγαπᾶτε"):
        assert fragment not in text
