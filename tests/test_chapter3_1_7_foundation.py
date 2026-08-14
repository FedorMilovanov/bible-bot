"""Foundation guards for Agent A's 1 Peter 3:1-7 lane."""
import re
from collections import Counter
from questions.chapter3.application_1_7 import APPLICATION_3_1_7 as A
from questions.chapter3.greek_1_7 import GREEK_3_1_7 as G,MORPHGNT_EVIDENCE_1P3 as M
from questions.chapter3.history_1_7 import HISTORY_3_1_7 as H
from questions.chapter3.intertext_1_7 import INTERTEXT_3_1_7 as O
from questions.chapter3.sources_1_7 import SOURCE_CATALOG as S,CONSERVATIVE_SOURCE_IDS as C,PRIMARY_SOCIAL_HISTORY_IDS as P,MODERN_SOCIAL_HISTORY_IDS as N
from questions.chapter3.text_1_7 import TEXT_3_1_7 as T
from questions.chapter3.theology_1_7 import THEOLOGY_3_1_7 as Y,DISPUTED_3_1_7 as D
ALL=T+G+O+H+Y+D+A

def test_contract_sources_ids():
 ids=[]
 for x in ALL:
  assert {"id","options","correct","claim_type","confidence","position","competitive","sources"}<=x.keys()
  assert re.fullmatch(r"ch3_(text|gr|ot|hist|theol|disp|app)_\d+",x["id"]); assert int(x["id"].rsplit("_",1)[1])>=101
  ids.append(x["id"]); src=set(x["sources"]); assert src<=S.keys()
  kinds={S[z]["kind"] for z in src}
  if x["claim_type"]=="text": assert {"primary_text_greek","primary_text_lxx"}&kinds
  if x["claim_type"]=="greek": assert {"sblgnt","morphgnt_1peter"}<=src
  if x["claim_type"]=="history": assert src&P and src&N
  if x["claim_type"]=="interpretation" and x["position"]=="project": assert len(src&C)>=2
  if x["confidence"]=="contested": assert not x["competitive"] and len(src)>=2
 assert len(ids)==len(set(ids))

def test_quiz_design():
 pos=Counter(); longest=0
 for x in ALL:
  o=x["options"]; c=x["correct"]; assert len(o)==len(set(o))==4 and 0<=c<4
  L=list(map(len,o)); assert max(L)/min(L)<=2.5,(x["id"],L); longest+=L[c]==max(L); pos[c]+=1
 assert set(pos)=={0,1,2,3} and max(pos.values())/len(ALL)<.4 and longest/len(ALL)<.6

TAGS={"ὁμοίως_3_1":"D---------","ὑποτασσόμεναι_3_1":"V-PPPNPF-","ἀπειθοῦσιν":"V-3PAI-P--","λόγῳ":"N-----DSM-","ἀναστροφῆς":"N-----GSF-","φόβῳ":"N-----DSM-","κόσμος":"N-----NSM-","πραέως":"A-----GSN-","ἡσυχίου":"A-----GSN-","ἐκόσμουν":"V-3IAI-P--","ὑπήκουσεν":"V-3AAI-S--","καλοῦσα":"V-PAPNSF-","φοβούμεναι":"V-PMPNPF-","πτόησιν":"N-----ASF-","συνοικοῦντες":"V-PAPNPM-","γνῶσιν":"N-----ASF-","ἀσθενεστέρῳ":"A-----DSNC","σκεύει":"N-----DSN-","γυναικείῳ":"A-----DSN-","συγκληρονόμοις":"A-----DPM-","ἐγκόπτεσθαι":"V-PPN----","προσευχὰς":"N-----APF-"}
def test_morphgnt_machine_tags():
 assert {k:v["tag"] for k,v in M.items()}==TAGS
 assert M["ὑποτασσόμεναι_3_1"]["lemma"]=="ὑποτάσσω" and M["ἀσθενεστέρῳ"]["lemma"]=="ἀσθενής"

def test_explicit_boundaries_and_semantic_control():
 cards={x["id"]:x for x in G}
 for i in (103,105,107,109,111,115,119): assert set(cards[f"ch3_gr_{i}"]["sources"])-{"sblgnt","morphgnt_1peter"}
 topics="|".join(x["topic"] for x in D)
 for z in ("φόβος","украш","Сарра","ἀσθενεστέρῳ σκεύει","κατὰ γνῶσιν","ὁμοίως"): assert z in topics
 assert all(x["confidence"]=="contested" and x["position"]=="neutral" and not x["competitive"] for x in D)

def test_noncompetitive_defaults_and_application_split():
 assert all(not x["competitive"] for x in G+H+O+Y+D+A)
 assert all(x["claim_type"]=="application" for x in A) and all(x["claim_type"]!="application" for x in T+G+H)
