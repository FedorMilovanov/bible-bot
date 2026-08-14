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