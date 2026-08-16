from pathlib import Path
from types import SimpleNamespace

import pytest

import question_identity


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
QUESTION_IDENTITY_SOURCE = (ROOT / "question_identity.py").read_text(encoding="utf-8")


def _legacy_stable(question: dict) -> str:
    import hashlib

    text = question.get("question", "")
    return hashlib.md5(text.encode()).hexdigest()[:12]


def _legacy_qid(question: dict) -> str:
    import hashlib

    text = question.get("question", "") + "".join(question.get("options", []))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def _legacy_namespace(**overrides):
    values = {
        "stable_question_id": _legacy_stable,
        "get_qid": _legacy_qid,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_known_question_identity_vectors_are_stable():
    cases = [
        ({"question": "", "options": []}, "d41d8cd98f00", "e3b0c44298fc"),
        (
            {
                "question": "Кто написал 1 Петра?",
                "options": ["Пётр", "Павел", "Иоанн", "Иаков"],
            },
            "0b476ad5ddf3",
            "43ddc179f338",
        ),
        (
            {"question": "Grace & truth", "options": ["A", "B"]},
            "7604e42999d9",
            "adbc57afe553",
        ),
    ]

    for question, stable_id, persisted_id in cases:
        assert question_identity.stable_question_id(question) == stable_id
        assert question_identity.get_qid(question) == persisted_id


def test_options_affect_persisted_qid_but_not_historical_stable_id():
    first = {"question": "same", "options": ["A", "B"]}
    second = {"question": "same", "options": ["B", "A"]}

    assert question_identity.stable_question_id(first) == question_identity.stable_question_id(second)
    assert question_identity.get_qid(first) != question_identity.get_qid(second)


def test_bridge_replaces_legacy_helpers_by_exact_identity_after_parity():
    legacy = _legacy_namespace()

    question_identity.install_legacy_bridge(legacy)

    assert legacy.stable_question_id is question_identity.stable_question_id
    assert legacy.get_qid is question_identity.get_qid

    question_identity.install_legacy_bridge(legacy)
    assert legacy.stable_question_id is question_identity.stable_question_id
    assert legacy.get_qid is question_identity.get_qid


def test_bridge_rejects_qid_drift_without_partial_mutation():
    original_stable = _legacy_stable

    def drifted_qid(_question: dict) -> str:
        return "0" * 12

    legacy = _legacy_namespace(get_qid=drifted_qid)

    with pytest.raises(RuntimeError, match="get_qid diverged"):
        question_identity.install_legacy_bridge(legacy)

    assert legacy.stable_question_id is original_stable
    assert legacy.get_qid is drifted_qid


def test_bridge_rejects_stable_id_drift_without_partial_mutation():
    def drifted_stable(_question: dict) -> str:
        return "f" * 12

    original_qid = _legacy_qid
    legacy = _legacy_namespace(stable_question_id=drifted_stable)

    with pytest.raises(RuntimeError, match="stable_question_id diverged"):
        question_identity.install_legacy_bridge(legacy)

    assert legacy.stable_question_id is drifted_stable
    assert legacy.get_qid is original_qid


@pytest.mark.parametrize(
    "legacy",
    [
        SimpleNamespace(get_qid=_legacy_qid),
        SimpleNamespace(stable_question_id=_legacy_stable),
        SimpleNamespace(stable_question_id=None, get_qid=_legacy_qid),
        SimpleNamespace(stable_question_id=_legacy_stable, get_qid=None),
    ],
)
def test_bridge_rejects_missing_or_noncallable_helpers(legacy):
    with pytest.raises(TypeError):
        question_identity.install_legacy_bridge(legacy)


def test_question_identity_has_no_legacy_import_or_runtime_state():
    assert "import bot" not in QUESTION_IDENTITY_SOURCE
    assert "from bot" not in QUESTION_IDENTITY_SOURCE
    assert "user_data" not in QUESTION_IDENTITY_SOURCE
    assert "Mongo" not in QUESTION_IDENTITY_SOURCE


def test_production_no_longer_installs_question_identity_bridge():
    assert "question_identity.install_legacy_bridge" not in PRODUCTION_SOURCE
    assert "import question_identity as question_identity" not in PRODUCTION_SOURCE
    assert "import bot" not in PRODUCTION_SOURCE
