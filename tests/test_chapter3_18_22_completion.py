import questions
from questions.chapter3.application_18_22 import APPLICATION_3_18_22 as A
from questions.chapter3.disputed_18_22 import DISPUTED_3_18_22 as D
from questions.chapter3.greek_18_22 import GREEK_3_18_22 as G
from questions.chapter3.intertext_18_22 import INTERTEXT_3_18_22 as I
from questions.chapter3.sources import SOURCE_CATALOG as S
from questions.chapter3.text_18_22 import TEXT_3_18_22 as T
from questions.chapter3.theology_18_22 import THEOLOGY_3_18_22 as H
ALL=T+G+D+I+H+A


def test_completion_boundary():
    known=set(questions.SOURCE_CATALOG)|set(S)
    assert len({x["id"] for x in ALL})==len(ALL)>=35
    for x in ALL:
        assert x["competitive"] is False and set(x["sources"])<=known
        assert "какая школа права" not in " ".join([x["question"],*x["options"]]).lower()


def test_direct_text_is_uninterpreted():
    for x in T:
        assert x["sources"]==["sblgnt"]
        assert (x["claim_type"],x["position"],x["evidence_layer"])==("text","neutral","text")


def test_morphgnt_snapshot():
    got={(x["morphgnt"]["form"],x["morphgnt"]["parse"]) for x in G}
    want={
      ("ἔπαθεν","3AAI-S--"),("προσαγάγῃ","3AAS-S--"),("ἐκήρυξεν","3AAI-S--"),
      ("διεσώθησαν","3API-P--"),("ἀντίτυπον","A- ----NSN-"),("ἐπερώτημα","N- ----NSN-"),
      ("ἅπαξ","D- --------"),("θανατωθεὶς","-APPNSM-"),("ζῳοποιηθεὶς","-APPNSM-"),
      ("ᾧ","RR ----DSN-"),("πνεύμασιν","N- ----DPN-"),("πορευθεὶς","-APPNSM-"),
      ("ἀπειθήσασίν","-AAPDPM-"),("σῴζει","3PAI-S--"),("ὑποταγέντων","-APPGPM-")}
    assert want<=got
    assert all({"sblgnt","morphgnt_1peter"}<=set(x["sources"]) for x in G)


def test_intertext_and_project_quorum():
    e=next(x for x in I if x["id"]=="ch3_int_003")
    assert {"enoch_10_14_charles","pierce_spirits_2011","grindheim_spirits_2024"}<=set(e["sources"])
    for x in [x for x in H if x["position"]=="project"]:
        src=set(x["sources"])
        assert any(v.startswith("gty_") for v in src)
        assert "schreiner_1peter_nac" in src
