from questions import POOL_REGISTRY


def _by_id(qid: str) -> dict:
    for pool in POOL_REGISTRY.values():
        for item in pool:
            if item.get("id") == qid:
                return item
    raise AssertionError(f"missing canonical question {qid}")


def test_geo_04_has_ephesus_as_the_answer():
    item = _by_id("geo_04")
    assert item["options"][item["correct"]] == "Эфес"
    assert "Эфес" in item["explanation"]


def test_ling2_12_uses_the_real_first_peter_1_6_sequence():
    item = _by_id("ling2_12")
    assert "ἐν ὀλίγον ἄρτι" not in item["question"]
    assert "ἐν ᾧ ἀγαλλιᾶσθε" in item["question"]


def test_ling2_15_does_not_invent_en_before_phthartois():
    item = _by_id("ling2_15")
    assert "ἐν φθαρτοῖς" not in item["question"]
    assert "φθαρτοῖς" in item["question"]


def test_ling3_06_uses_agapesate_and_correct_morphology():
    item = _by_id("ling3_06")
    assert "ἀγαπᾶτε" not in item["question"]
    assert "ἀγαπήσατε" in item["question"]
    answer = item["options"][item["correct"]]
    assert "Аористный активный императив" in answer
