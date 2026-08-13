# First Peter 2-5 Completion Roadmap

This roadmap defines what "complete through chapter 5" means. Question count alone is not sufficient.

## Architecture for new chapters

Do not repeat the chapter-1 monolith. Prefer one package per chapter:

```text
questions/chapter2/
  __init__.py
  text.py
  greek.py
  intertext.py
  history.py
  theology.py
  disputed.py
  application.py
```

Use the same shape for chapters 3-5. Production handlers consume only canonical pools assembled by `questions/__init__.py`; they must not import these domain files directly.

Stable ID families:

- `ch2_text_001`
- `ch2_gr_001`
- `ch2_ot_001`
- `ch2_hist_001`
- `ch2_theol_001`
- `ch2_disputed_001`
- `ch2_app_001`

Equivalent prefixes apply to chapters 3-5.

## Chapter 2 coverage

### 2:1-3 — longing for the word / growth

Required:

- direct text and imperatives;
- `apothemenoi`, `epipothesate`, `logikon/adolon` lexical review;
- Psalm 34:8 / "taste that the Lord is good" intertext;
- interpretation of `logikon` without one-gloss overclaiming;
- application kept non-competitive.

### 2:4-10 — living stones and people of God

Required:

- Christ as living/choice cornerstone;
- believers as living stones;
- holy/royal priesthood;
- Isa 28:16, Ps 118:22, Isa 8:14, Exod 19:5-6, Isaiah/Hosea intertexts;
- `lithos`, `akrogoniaios`, `hierateuma`, `peripoiesis` Greek work;
- continuity/application to the church presented with appropriate interpretive metadata.

Create an OT-intertext module, not isolated trivia.

### 2:11-17 — aliens/exiles, witness, civil authority

Required:

- passions/witness/good works;
- `paroikoi/parepidemoi` lexical review;
- social honor/shame context;
- civil authority and `for the Lord's sake`;
- scope/limits of application separated from the direct command.

### 2:18-25 — household slaves, unjust suffering, Christ's example/atonement

Required:

- historical `oiketai/despotai` context; do not define this as modern employment;
- unjust suffering and conscience toward God;
- Christ's sinlessness and example;
- Isa 53 intertext;
- 2:24 substitution/atonement;
- `molops`, `xylon`, `episkopos`, shepherd imagery;
- TMS/MacArthur conservative penal-substitution position plus independent evangelical exegetical control;
- historical and application questions clearly separated.

## Chapter 3 coverage

### 3:1-7 — wives/husbands and household context

Required:

- direct commands and stated purpose;
- Greco-Roman marriage/household context;
- Sarah/Abraham reference;
- `skeuos`, `synkleronomoi`, `time` lexical/context review;
- avoid making one modern pastoral application identical to the ancient social category;
- project theological/ethical conclusions labelled where needed.

### 3:8-12 — community ethics and Psalm 34

Required:

- unity, sympathy, brotherly love, compassion, humility;
- blessing instead of retaliation;
- Psalm 34 quotation and LXX relationship;
- text/OT questions may be competitive when unambiguous.

### 3:13-17 — suffering and Christian defense

Required:

- `apologia`;
- sanctifying Christ as Lord;
- Isaiah 8:12-13 intertext and Christological significance;
- gentleness/good conscience;
- distinguish direct text from systematic Christology while allowing the project's conservative conclusion.

### 3:18-22 — atonement, spirits, Noah, baptism, exaltation

This is a mandatory disputed-passages laboratory.

Required:

- 3:18 substitution (`dikaios hyper adikon`, `hapax`, `prosagage`);
- body/spirit clause syntax;
- 3:19-20 major interpretations, including MacArthur/TMS-style fallen-spirit proclamation and Grudem's Noah-related reading;
- do not force a disputed interpretation into ranking;
- Noah/flood textual facts;
- `antitypon`, `eperotema`, `syneidesis` in 3:21;
- baptismal-regeneration question handled as interpretation, not lexical fiat;
- resurrection/ascension/subjection of powers in 3:21-22.

## Chapter 4 coverage

### 4:1-6 — suffering, former life, judgement, the dead

Required:

- arm yourselves with Christ's attitude;
- former Gentile practices and social reaction;
- judgement of living/dead;
- 4:6 as a mandatory disputed module with multiple serious readings;
- conservative project position labelled, never competitive as interpretation.

### 4:7-11 — eschatology, prayer, love, hospitality, gifts

Required:

- "the end is near" in first-century Christian eschatological discourse;
- sober-minded prayer;
- Prov 10:12 / love covers sins discussion where relevant;
- hospitality without grumbling;
- gifts/stewardship;
- `logia theou` and serving strength supplied by God;
- purpose clause: God glorified through Jesus Christ.

### 4:12-19 — fiery trial and suffering as Christian

Required:

- fiery trial;
- participation in Christ's sufferings;
- Spirit of glory/God;
- murder/thief/evildoer/meddler distinctions;
- name `Christian` and its historical/social significance;
- judgement beginning with God's household;
- 4:19 entrusting souls to a faithful Creator while doing good;
- TMS/MacArthur suffering theology plus historical control.

## Chapter 5 coverage

### 5:1-4 — elders and shepherding

Required:

- `presbyteros`, `poimaino`, `episkopeo`;
- relationship among elder/shepherd/overseer vocabulary;
- willing vs compelled service;
- shameful gain;
- not domineering, but examples;
- `archipoimen` / Chief Shepherd and unfading crown;
- pastoral theology may use TMS heavily but Greek claims require Greek authority.

### 5:5-7 — humility and anxiety

Required:

- younger/submission/humility;
- Prov 3:34 LXX quotation;
- mighty hand of God;
- casting anxiety on God because he cares;
- syntax of the participial relationship reviewed before teaching a precise grammatical conclusion.

### 5:8-11 — adversary, suffering, restoration

Required:

- sober/watchful;
- devil/adversary/roaring lion imagery;
- resist firm in faith;
- worldwide brotherhood suffering;
- God of all grace/restoration;
- avoid sensational demonology beyond the text.

### 5:12-14 — Silvanus, Babylon, Mark, final greeting

Required:

- Silvanus wording and role ambiguity;
- Babylon=Rome as widespread/traditional reconstruction, not self-decoding text;
- Mark and later Petrine tradition distinguished from direct verse wording;
- kiss of love / peace conclusion;
- dating/authorship/location remain project/contested where appropriate.

## Cross-chapter special modes

The completed course should support these conceptual collections even if the first implementation exposes them as normal pools:

- `OT inside 1 Peter` — explicit quotation/allusion map;
- `Greek Lab` — source-backed morphology/semantics;
- `Disputed Passages` — major views + course position;
- `Historical Lab` — Rome, Asia Minor, household, slavery, persecution, early church evidence;
- `Pastoral Ministry` — especially chapter 5;
- `Suffering and Witness` — cross-chapter argument from 2-5.

## Coverage targets

Do not optimize for an arbitrary total, but the expected depth is roughly:

- chapter 2: 90-120 canonical items;
- chapter 3: 100-130 canonical items because of the disputed 3:18-22 block;
- chapter 4: 80-110 canonical items;
- chapter 5: 70-100 canonical items.

These are planning ranges, not a licence to pad the bank with trivia.

## Per-chapter definition of done

A chapter may be called complete only when:

1. Every verse/pericope is intentionally represented in the coverage matrix.
2. All Greek forms used as evidence are checked against SBLGNT/MorphGNT.
3. OT quotations/allusions are classified and sourced.
4. Historical claims meet primary+modern-control policy when possible.
5. Major conservative commentary has been consulted for theological modules.
6. Serious alternative readings are represented for genuinely disputed passages.
7. Every canonical item has valid metadata and source IDs.
8. Application and contested interpretation are outside competitive ranking.
9. Structural/source-truth regression tests pass.
10. Full repository CI, Security Audit, and CodeQL are green on the exact PR merge result.

## Whole-book definition of done

1 Peter is complete only after chapters 1-5 share the same canonical metadata/evidence standard, the cross-chapter modes are derivable from metadata rather than duplicated banks, and the full book passes a final source-truth marathon before merge to `main`.