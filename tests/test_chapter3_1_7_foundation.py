"""Foundation guards for Agent A's 1 Peter 3:1-7 lane."""
import re
from collections import Counter

import questions.chapter3.application_1_7 as a
import questions.chapter3.greek_1_7 as g
import questions.chapter3.history_1_7 as h
import questions.chapter3.intertext_1_7 as o
import questions.chapter3.sources_1_7 as s
import questions.chapter3.text_1_7 as t
import questions.chapter3.theology_1_7 as y

G = g.GREEK_3_1_7
D = y.DISPUTED_3_1_7
A = a.APPLICATION_3_1_7
H = h.HISTORY_3_1_7
OT = o.INTERTEXT_3_1_7
Y = y.THEOLOGY_3_1_7
T = t.TEXT_3_1_7
ALL = T + G + OT + H + Y + D + A


def test_contract_sources_ids():
    ids = []
    for x in ALL:
        required = {"id", "options", "correct", "claim_type", "confidence", "position", "competitive", "sources"}
        assert required <= x.keys()
        assert re.fullmatch(r"ch3_(text|gr|ot|hist|theol|disp|app)_\d+", x["id"])
        assert int(x["id"].rsplit("_", 1)[1]) >= 101
        ids.append(x["id"])
        sources = set(x["sources"])
        assert sources <= s.SOURCE_CATALOG.keys()
        kinds = {s.SOURCE_CATALOG[z]["kind"] for z in sources}
        if x["claim_type"] == "text":
            assert {"primary_text_greek", "primary_text_lxx"} & kinds
        if x["claim_type"] == "greek":
            assert {"sblgnt", "morphgnt_1peter"} <= sources
        if x["claim_type"] == "history":
            assert sources & s.PRIMARY_SOCIAL_HISTORY_IDS
            assert sources & s.MODERN_SOCIAL_HISTORY_IDS
        if x["claim_type"] == "interpretation" and x["position"] == "project":
            # URL/bibliographic existence is not evidence quorum: require two
            # passage-level conservative witnesses actually inspected in this lane.
            assert len(sources & s.INSPECTED_CONSERVATIVE_SOURCE_IDS) >= 2
        if x["confidence"] == "contested":
            assert not x["competitive"]
            assert len(sources) >= 2
    assert len(ids) == len(set(ids))


def test_quiz_design():
    pos = Counter()
    longest = 0
    for x in ALL:
        options = x["options"]
        correct = x["correct"]
        assert len(options) == len(set(options)) == 4
        assert 0 <= correct < 4
        lengths = list(map(len, options))
        assert max(lengths) / min(lengths) <= 2.5
        longest += lengths[correct] == max(lengths)
        pos[correct] += 1
    assert set(pos) == {0, 1, 2, 3}
    assert max(pos.values()) / len(ALL) < 0.4
    assert longest / len(ALL) < 0.6


def test_morphgnt_machine_fixture():
    """Compare every stored form against the frozen MorphGNT 1 Pet 3:1-7 fixture."""
    expected = [
        ("ὁμοίως_3_1", "1 Пет. 3:1", "Ὁμοίως", "ὁμοίως", "D-", "--------"),
        ("ὑποτασσόμεναι_3_1", "1 Пет. 3:1", "ὑποτασσόμεναι", "ὑποτάσσω", "V-", "-PPPNPF-"),
        ("ἀπειθοῦσιν", "1 Пет. 3:1", "ἀπειθοῦσιν", "ἀπειθέω", "V-", "3PAI-P--"),
        ("λόγῳ", "1 Пет. 3:1", "λόγῳ", "λόγος", "N-", "----DSM-"),
        ("ἀναστροφῆς", "1 Пет. 3:1", "ἀναστροφῆς", "ἀναστροφή", "N-", "----GSF-"),
        ("φόβῳ", "1 Пет. 3:2", "φόβῳ", "φόβος", "N-", "----DSM-"),
        ("κόσμος", "1 Пет. 3:3", "κόσμος", "κόσμος", "N-", "----NSM-"),
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
        ("συγκληρονόμοις", "1 Пет. 3:7", "συγκληρονόμοις", "συγκληρονόμος", "A-", "----DPM-"),
        ("ἐγκόπτεσθαι", "1 Пет. 3:7", "ἐγκόπτεσθαι", "ἐγκόπτω", "V-", "-PPN----"),
        ("προσευχὰς", "1 Пет. 3:7", "προσευχὰς", "προσευχή", "N-", "----APF-"),
    ]
    morph = g.MORPHGNT_EVIDENCE_1P3
    actual = [
        (key, row["verse"], row["surface"], row["lemma"], row["pos"], row["parse"])
        for key, row in morph.items()
    ]
    assert actual == expected
    assert morph["ὁμοίως_3_1"]["tag"] == "D- --------"
    assert morph["ὁμοίως_3_7"]["tag"] == "D- --------"
    assert morph["ὑποτασσόμεναι_3_1"]["tag"] == "V- -PPPNPF-"
    assert morph["ἀσθενεστέρῳ"]["lemma"] == "ἀσθενής"


def test_explicit_boundaries_and_semantic_control():
    cards = {x["id"]: x for x in G}
    for n in (101, 103, 105, 107, 109, 111, 113, 115, 117, 119):
        assert set(cards[f"ch3_gr_{n}"]["sources"]) - {"sblgnt", "morphgnt_1peter"}
    topics = "|".join(x["topic"] for x in D)
    markers = ("φόβος", "\u0443\u043a\u0440\u0430\u0448", "\u0421\u0430\u0440\u0440\u0430", "ἀσθενεστέρῳ σκεύει", "κατὰ γνῶσιν", "ὁμοίως")
    assert all(z in topics for z in markers)
    assert all(x["confidence"] == "contested" for x in D)
    assert all(x["position"] == "neutral" and not x["competitive"] for x in D)


def test_noncompetitive_defaults_and_application_split():
    assert all(not x["competitive"] for x in G + H + OT + Y + D + A)
    assert all(x["claim_type"] == "application" for x in A)
    assert all(x["claim_type"] != "application" for x in T + G + H)
