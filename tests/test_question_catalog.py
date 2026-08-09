from collections import Counter

from questions import POOL_REGISTRY


LEAF_POOLS = (
    "easy_p1", "easy_p2",
    "medium_p1", "medium_p2",
    "hard_p1", "hard_p2",
    "practical_p1", "practical_p2",
    "linguistics_ch1", "linguistics_ch1_2", "linguistics_ch1_3",
    "nero", "geography",
    "intro1", "intro2", "intro3",
)


def test_leaf_pools_are_large_enough_for_standard_quiz():
    too_small = {key: len(POOL_REGISTRY[key]) for key in LEAF_POOLS if len(POOL_REGISTRY[key]) < 10}
    assert too_small == {}


def test_canonical_questions_have_valid_schema_and_answers():
    errors = []
    for pool_key in LEAF_POOLS:
        for index, question in enumerate(POOL_REGISTRY[pool_key]):
            label = f"{pool_key}[{index}]"
            qid = question.get("id")
            text = question.get("question")
            options = question.get("options")
            correct = question.get("correct")

            if not isinstance(qid, str) or not qid.strip():
                errors.append(f"{label}: missing id")
            if not isinstance(text, str) or not text.strip():
                errors.append(f"{label}: missing question text")
            if not isinstance(options, list) or len(options) < 2:
                errors.append(f"{label}: fewer than two options")
                continue
            if any(not isinstance(option, str) or not option.strip() for option in options):
                errors.append(f"{label}: empty/non-string option")
            normalized = [option.strip().casefold() for option in options if isinstance(option, str)]
            if len(normalized) != len(set(normalized)):
                errors.append(f"{label}: duplicate answer option")
            if not isinstance(correct, int) or isinstance(correct, bool) or not 0 <= correct < len(options):
                errors.append(f"{label}: invalid correct index {correct!r}")

    assert errors == []


def test_canonical_question_ids_are_globally_unique():
    ids = [question["id"] for key in LEAF_POOLS for question in POOL_REGISTRY[key]]
    duplicates = sorted(qid for qid, count in Counter(ids).items() if count > 1)
    assert duplicates == []


def test_random_all_contains_each_leaf_question_once():
    leaf_ids = {question["id"] for key in LEAF_POOLS for question in POOL_REGISTRY[key]}
    random_ids = [question["id"] for question in POOL_REGISTRY["random_all"]]
    assert len(random_ids) == len(set(random_ids))
    assert set(random_ids) == leaf_ids
    assert len(random_ids) >= 20
