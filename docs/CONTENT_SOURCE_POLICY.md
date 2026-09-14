# Content Source Policy

`AGENTS.md` is mandatory. This file defines the evidence standard for production questions.

## Project identity

The course is conservative evangelical. It may teach traditional/project conclusions, but it must distinguish direct biblical text, Greek evidence, historical evidence, interpretation, and application. A conservative conclusion is not automatically a neutral lexical or historical fact.

## Evidence hierarchy

### Biblical text and Greek

- Greek surface text: SBLGNT.
- Morphology/parsing: MorphGNT/SBLGNT.
- Semantic/syntactic claims: Greek text plus serious lexical, grammatical, and exegetical control.
- Never create Greek forms from memory.
- Never infer a full theological meaning from a tense label alone.

### Conservative exposition

Preferred serious witnesses include TMS / John MacArthur, Thomas Schreiner, Karen Jobes, Wayne Grudem, Peter Davids, and Craig Keener where historical/contextual control is useful.

TMS/MacArthur may define the project's conservative doctrinal position, but one expositor is not enough to establish neutral Greek grammar, ancient history, or scholarly consensus.

### Historical evidence

Prefer a relevant primary witness plus modern scholarly control. Examples include Tacitus, Suetonius, Pliny/Trajan, and later church-historical testimony such as Eusebius with its later date clearly identified.

Use `likely`, `probable`, `later tradition`, and `one reconstruction` when those words match the actual evidence.

### Broader scholarly control

For historically or exegetically disputed claims use reputable independent scholarship such as Oxford, Cambridge, NTS, JTS, and major academic commentaries or monographs. This protects intellectual honesty without changing the conservative identity of the course.

## Minimum quorum

- `text`: canonical biblical text.
- morphology: SBLGNT plus MorphGNT.
- Greek semantics/syntax: text/morphology plus at least one serious exegetical or lexical source; add an independent second control for non-trivial claims.
- history: primary source when available plus modern scholarly/reference control.
- project theology: biblical evidence plus at least two serious conservative/evangelical witnesses when practical.
- disputed passage: understand at least two materially different serious interpretations before publishing a forced-answer item.

If quorum is not met, lower confidence, mark the item contested and non-competitive, or do not publish it.

## Machine-checked Greek evidence

A parsing or quotation claim is checked against the corpus, not against memory:

- `scripts/verify_greek_evidence.py` verifies every `morphgnt` metadata claim of
  the bank (form, parse, lemma) and every Greek form quoted in the stem of a
  1 Peter-anchored `text`/`greek` card.
- `data/morphgnt-1peter-evidence.json` holds the corpus rows those claims rest
  on. It is an excerpt of the upstream MorphGNT/SBLGNT file and records the
  upstream URL, the SHA-256 of the file it was read from, and the row counts, so
  the table can be re-derived.
- `tests/test_greek_evidence.py` runs the check offline and pins the editorial
  corrections that came out of it (a genitive read as a dative, a nominative read
  as a dative plural, `κάλυμμα` for `ἐπικάλυμμα`, `τιμία` for `τιμίῳ`, a
  nominative title for the dative of 4:19, and two option-level citations).

Review-time refresh, after fetching `81-1Pe-morphgnt.txt`:

```bash
python scripts/verify_greek_evidence.py --corpus /path/to/81-1Pe-morphgnt.txt
python scripts/verify_greek_evidence.py --corpus /path/to/81-1Pe-morphgnt.txt --write-evidence
```

Attribution. The vendored excerpt is a small verbatim selection of the upstream rows and
records its citation (Tauber, J. K., ed. (2017) *MorphGNT: SBLGNT Edition*, v6.12,
DOI 10.5281/zenodo.376200) and the upstream licence split: the SBLGNT text is subject to
the SBLGNT EULA, the parsing and lemmatization are released under CC BY-SA 3.0. It is the
only vendored corpus - precisely because the Old Testament dataset's terms do not allow
the same treatment. Review both files if the project ever changes licence or starts
charging for the course.

Matching tolerates the two differences that do not change the form being cited:
a word quoted out of its sentence carries an acute where the running text has a
grave, and a movable nu may be printed or dropped. Accents that mark a different
form (the circumflex of `διασπορᾶς`, breathing marks) are not tolerated, and a
form the corpus does not contain is reported.

### Old Testament / LXX quotations

The other half of the bank's Greek is quoted from the Septuagint, so it is checked
the same way, against a Rahlfs 1935 corpus:

- `scripts/verify_lxx_evidence.py --corpus <text> --versification <map>` reads
  every Old Testament reference off a card (the anchor plus inline citations such
  as `Притч. 3:25`), loads those verses, and reports Greek that is in neither the
  cited verses nor 1 Peter. An option that names a reference and quotes next to it
  is checked against that reference, which is how a misattributed quotation shows
  up.
- The corpus text is **not** vendored: the upstream dataset is CC BY-NC-SA 4.0 and
  derives from CCAT material with its own access terms. What is committed is
  `data/ot-citation-fingerprints.json` - the references each card cites, the
  SHA-256 of the Greek it quotes, and the SHA-256 of the corpus the run used.
- `tests/test_ot_citations.py` runs offline: if the Greek of one of those cards
  changed, it fails and asks for a fresh corpus run, so a citation cannot drift
  away from the text it was verified against.

## Required metadata

Every canonical production item must resolve:

- `claim_type`: `text | greek | history | interpretation | application`;
- `confidence`: `high | medium | contested`;
- `position`: `neutral | project`;
- `competitive`: boolean;
- `sources`: canonical source IDs.

Important project-position questions must also identify that position visibly in user-facing wording or explanation.

## Disputed passages

These passages require explicit disputed-passage treatment:

- `1 Pet 3:19-20` — the spirits in prison and Christ's proclamation;
- `1 Pet 3:21` — baptism and the difficult terms surrounding its explanation;
- `1 Pet 4:6` — the dead and the timing of gospel proclamation.

For each disputed passage:

1. state the exegetical problem neutrally;
2. record the major viable interpretations;
3. record the relevant textual and grammatical arguments;
4. state the course position if one is adopted;
5. keep interpretation questions non-competitive;
6. permit competitive questions only for undisputed surrounding facts.

A disputed module should teach why a passage is difficult, not hide disagreement behind one commentator's answer.

## Old Testament / LXX intertext

Classify each relationship as one of:

- explicit quotation;
- clear verbal allusion;
- probable background;
- thematic parallel.

Do not call a proposed background an explicit quotation. Record both the 1 Peter passage and the OT/LXX source, including numbering differences when relevant.

## Social history

Do not flatten ancient institutions into modern analogies. For example, `oiketai` in 1 Pet 2:18 belongs to an ancient household-slavery/dependence context. Application to modern employment must be a separate application question, not the historical definition.

## Competitive standard

Default `competitive=false` for application, genuinely disputed interpretation, authorship/date reconstruction, proposed allusion rather than explicit quotation, pastoral judgement scenarios, and complex Greek claims before explicit source review.

Never enlarge ranking pools by weakening evidence standards.

## Copyright and source hygiene

- Store bibliographic metadata and source IDs, not copied commentary chapters.
- Paraphrase modern commentary unless a short quotation is necessary.
- Attribute ancient/public-domain sources accurately.

## Completion rule

A chapter is not complete because it has many questions. It is complete only when its coverage matrix, source metadata, Greek review, historical review, disputed-passages review, tests, and exact-head CI/Security/CodeQL are complete.