"""Foundation and editorial guards for Agent A's 1 Peter 3:1-7 lane."""

import re
import unicodedata
from collections import Counter
from itertools import combinations

import questions.chapter3.application_1_7 as application_bank
import questions.chapter3.greek_1_7 as greek_bank
import questions.chapter3.history_1_7 as history_bank
import questions.chapter3.intertext_1_7 as intertext_bank
import questions.chapter3.sources_1_7 as source_bank
import questions.chapter3.text_1_7 as text_bank
import questions.chapter3.theology_1_7 as theology_bank

TEXT_CARDS = text_bank.TEXT_3_1_7
GREEK_CARDS = greek_bank.GREEK_3_1_7
INTERTEXT_CARDS = intertext_bank.INTERTEXT_3_1_7
HISTORY_CARDS = history_bank.HISTORY_3_1_7
THEOLOGY_CARDS = theology_bank.THEOLOGY_3_1_7
DISPUTED_CARDS = theology_bank.DISPUTED_3_1_7
APPLICATION_CARDS = application_bank.APPLICATION_3_1_7
ALL_CARDS = (
    TEXT_CARDS
    + GREEK_CARDS
    + INTERTEXT_CARDS
    + HISTORY_CARDS
    + THEOLOGY_CARDS
    + DISPUTED_CARDS
    + APPLICATION_CARDS
)

CLAIM_TYPES = {"text", "greek", "history", "interpretation", "application"}
CONFIDENCE_VALUES = {"high", "medium", "contested"}
POSITION_VALUES = {"neutral", "project"}


def _normalize(value):
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return " ".join(re.findall(r"\w+", normalized, flags=re.UNICODE))


def _token_jaccard(left, right):
    left_tokens = set(_normalize(left).split())
    right_tokens = set(_normalize(right).split())
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def test_canonical_metadata_contract_and_source_resolution():
    required = {
        "id",
        "question",
        "options",
        "correct",
        "explanation",
        "verse",
        "topic",
        "claim_type",
        "confidence",
        "position",
        "competitive",
        "sources",
    }
    ids = []

    for card in ALL_CARDS:
        assert required <= card.keys()
        assert re.fullmatch(r"ch3_(text|gr|ot|hist|theol|disp|app)_\d+", card["id"])
        assert int(card["id"].rsplit("_", 1)[1]) >= 101
        assert card["claim_type"] in CLAIM_TYPES
        assert card["confidence"] in CONFIDENCE_VALUES
        assert card["position"] in POSITION_VALUES
        assert isinstance(card["competitive"], bool)

        ids.append(card["id"])
        sources = set(card["sources"])
        assert sources
        assert sources <= source_bank.SOURCE_CATALOG.keys()

        if card["claim_type"] == "text":
            source_kinds = {source_bank.SOURCE_CATALOG[source_id]["kind"] for source_id in sources}
            assert {"primary_text_greek", "primary_text_lxx"} & source_kinds

        if card["claim_type"] == "greek":
            assert {"sblgnt", "morphgnt_1peter"} <= sources

        if card["claim_type"] == "history":
            assert sources & source_bank.PRIMARY_SOCIAL_HISTORY_IDS
            assert sources & source_bank.MODERN_SOCIAL_HISTORY_IDS

        if card["position"] == "project":
            assert len(sources & source_bank.INSPECTED_CONSERVATIVE_SOURCE_IDS) >= 2

        must_be_noncompetitive = (
            card["claim_type"] in {"greek", "history", "application"}
            or card["position"] == "project"
            or card["confidence"] == "contested"
        )
        if must_be_noncompetitive:
            assert card["competitive"] is False

    assert len(ids) == len(set(ids))


def test_source_inspection_status_is_explicit_and_bibliographic_only_sources_do_not_prove_cards():
    for metadata in source_bank.SOURCE_CATALOG.values():
        assert metadata["evidence_status"] in source_bank.EVIDENCE_STATUS_VALUES

    bibliographic_only = {
        source_id
        for source_id, metadata in source_bank.SOURCE_CATALOG.items()
        if metadata["evidence_status"] in {"bibliographic_only", "bibliographic_toc_only"}
    }
    assert bibliographic_only
    assert bibliographic_only <= source_bank.LIMITED_EVIDENCE_SOURCE_IDS

    for card in ALL_CARDS:
        assert not (set(card["sources"]) & bibliographic_only)


def test_editorial_option_and_question_uniqueness():
    normalized_questions = []
    correct_positions = Counter()
    correct_is_longest = 0

    for card in ALL_CARDS:
        options = card["options"]
        correct = card["correct"]

        assert len(options) == 4
        assert 0 <= correct < len(options)

        normalized_options = [_normalize(option) for option in options]
        assert all(normalized_options)
        assert len(normalized_options) == len(set(normalized_options))

        option_lengths = [len(option) for option in normalized_options]
        assert max(option_lengths) / min(option_lengths) <= 2.5

        correct_positions[correct] += 1
        correct_is_longest += option_lengths[correct] == max(option_lengths)
        normalized_questions.append(_normalize(card["question"]))

    assert len(normalized_questions) == len(set(normalized_questions))
    assert set(correct_positions) == {0, 1, 2, 3}
    assert max(correct_positions.values()) / len(ALL_CARDS) < 0.35
    assert correct_is_longest / len(ALL_CARDS) < 0.55


def test_no_extreme_near_duplicate_questions():
    for left, right in combinations(ALL_CARDS, 2):
        similarity = _token_jaccard(left["question"], right["question"])
        assert similarity < 0.9, (
            f"near-duplicate questions: {left['id']} / {right['id']} "
            f"(token Jaccard={similarity:.2f})"
        )


def test_morphgnt_machine_fixture():
    """Compare every stored form against the frozen MorphGNT 1 Pet 3:1-7 slice."""
    expected = [
        ("ὁμοίως_3_1", "1 Пет. 3:1", "Ὁμοίως", "ὁμοίως", "D-", "--------"),
        ("ὑποτασσόμεναι_3_1", "1 Пет. 3:1", "ὑποτασσόμεναι", "ὑποτάσσω", "V-", "-PPPNPF-"),
        ("ἀπειθοῦσιν", "1 Пет. 3:1", "ἀπειθοῦσιν", "ἀπειθέω", "V-", "3PAI-P--"),
        ("λόγῳ", "1 Пет. 3:1", "λόγῳ", "λόγος", "N-", "----DSM-"),
        ("ἀναστροφῆς", "1 Пет. 3:1", "ἀναστροφῆς", "ἀναστροφή", "N-", "----GSF-"),
        ("φόβῳ", "1 Пет. 3:2", "φόβῳ", "φόβος", "N-", "----DSM-"),
        ("κόσμος", "1 Пет. 3:3", "κόσμος", "N-", "----NSM-"),
        ("πραέως", "1 Пет. 3:4", "πραέως", "πραΰς", "A-", "----GSN-"),
        ("ἡσυχίου", "1 Пет. 3:4", "ἡσυχίου", "ἡσύχιος", "A-", "----GSN-"),
        ("ἐκόσμουν", "1 Пет. 3:5", "ἐκόσμουν", "κοσμέω", "V-", "3IAI-P--"),
        ("ὑπήκουσεν", "1 Пет. 3:6", "ὑπήκουσεν", "ὑπακούω", "V-", "3AAI-S--"),
        ("καλοῦσα", "1 Пет. 3:6", "καλοῦσα", "καλέω", "V-", "-PAPNSF-"),
        ("φοβούμεναι", "1 Пет. 3:6", "φοβούμεναι", "φοβέομαι", "V-", "-PMPNPF-"),
        ("πτόησιν", "1 Пет. 3:6", "πτόησιν", "πτόησις", "N-", "----ASF-"),
        ("ὁμοίως_3_7", "1 Пет. 3:7", "ὁμοίως", "ὁμοίως", "D-", "--------"),
        ("συνοικοῦντες", "1 Пет. 3:7", "συνοικοῦντες", "συνοικέω", "V-", "-PAPNPM-"),
        ("γνῶσιν", "1 Пет. 3:7", "γνῶσιν", "γνῶσις", "N-", "----ASF-"),
        ("ἀσθενεστέρῳ", "1 Пет. 3:7", "ἀσθενεστέρῳ", "ἀσθενής", "A-", "----DSNC"),
        ("σκεύει", "1 Пет. 3:7", "σκεύει", "σκεῦος", "N-", "----DSN-"),
        ("γυναικείῳ", "1 Пет. 3:7", "γυναικείῳ", "γυναικεῖος", "A-", "----DSN-"),
        ("συγκληρονόμοις", "1 Пет. 3:7", "συγκληρονόμοις", "A-", "----DPM-"),
        ("ἐγκόπτεσθαι", "1 Пет. 3:7", "ἐγκόπτεσθαι", "ἐγκόπτω", "V-", "-PPN----"),
        ("προσευχὰς", "1 Пет. 3:7", "προσευχὰς", "προσευχή", "N-", "----APF-"),
    ]

    actual = [
        (key, row["verse"], row["surface"], row["lemma"], row["pos"], row["parse"])
        for key, row in greek_bank.MORPHGNT_EVIDENCE_1P3.items()
    ]
    assert actual == expected


def test_disputed_cards_are_structurally_held_open():
    expected_ids = {
        "ch3_disp_101",
        "ch3_disp_102",
        "ch3_disp_103",
        "ch3_disp_104",
        "ch3_disp_105",
        "ch3_disp_106",
    }
    assert {card["id"] for card in DISPUTED_CARDS} == expected_ids
    assert all(card["claim_type"] == "interpretation" for card in DISPUTED_CARDS)
    assert all(card["confidence"] == "contested" for card in DISPUTED_CARDS)
    assert all(card["position"] == "neutral" for card in DISPUTED_CARDS)
    assert all(card["competitive"] is False for card in DISPUTED_CARDS)
    assert all(len(set(card["sources"])) >= 3 for card in DISPUTED_CARDS)


def test_application_is_separate_from_factual_layers():
    assert all(card["claim_type"] == "application" for card in APPLICATION_CARDS)
    assert all(card["position"] == "project" for card in APPLICATION_CARDS)
    assert all(card["competitive"] is False for card in APPLICATION_CARDS)
    assert all(card["claim_type"] != "application" for card in TEXT_CARDS + GREEK_CARDS + HISTORY_CARDS)
