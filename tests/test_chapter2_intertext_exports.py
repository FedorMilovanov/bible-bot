from questions.chapter2 import INTERTEXT_2_1_10


def test_chapter2_intertext_export_includes_hosea_2_10():
    ids = {item["id"] for item in INTERTEXT_2_1_10}
    assert "ch2_ot_006" in ids
