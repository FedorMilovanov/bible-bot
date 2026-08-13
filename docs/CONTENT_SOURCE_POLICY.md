# Content Source Policy

`AGENTS.md` is mandatory. This file defines the evidence standard for questions.

## Project identity

The course is conservative evangelical. It may teach the project's traditional conclusions, but it must distinguish direct text, Greek evidence, historical evidence, interpretation, and application.

A conservative conclusion is not automatically a neutral lexical or historical fact.

## Evidence hierarchy

### Biblical text and Greek

- Greek surface text: SBLGNT.
- Morphology/parsing: MorphGNT/SBLGNT.
- Semantic/syntactic claims: Greek text + serious lexical/grammatical/exegetical control.
- Never create Greek forms from memory.
- Never use shortcuts such as `aorist = completed theological fact` without contextual evidence.

### Conservative exposition

Preferred serious witnesses include:

- TMS / John MacArthur;
- Thomas Schreiner;
- Karen Jobes;
- Wayne Grudem;
- Peter Davids;
- Craig Keener where historical/contextual control is useful.

TMS/MacArthur may define the project's conservative doctrinal position, but one expositor is not enough to establish neutral Greek grammar, ancient history, or scholarly consensus.

### Historical evidence

Prefer a relevant primary witness plus modern scholarly control.

Examples:

- Tacitus for Nero and Christians in Rome after the fire;
- Suetonius for claims specifically reported by Suetonius;
- Pliny/Trajan for early-second-century administrative practice in Bithynia-Pontus;
- Eusebius as later church-historical testimony, not contemporary Roman documentation.

Use `likely`, `probable`, `later tradition`, or `one reconstruction` where the evidence requires it.

### Broader scholarly control

For historically or exegetically disputed claims use reputable independent scholarship (Oxford, Cambridge, NTS, JTS, major academic commentaries/monographs) to identify overstatement and serious alternative readings.

The purpose is intellectual honesty, not doctrinal neutrality.

## Minimum quorum

- `text`: canonical biblical text.
- morphology: SBLGNT + MorphGNT.
- Greek semantics/syntax: text/morphology + at least one serious exegetical/lexical source; add an independent second source for non-trivial claims.
- history: primary source when available + modern scholarly/reference control.
- project theology: biblical evidence + at least two serious conservative/evangelical witnesses when practical.
- disputed passage: understand at least two materially different serious interpretations before publishing a forced-answer item.

If quorum is not met, lower confidence, mark the item contested/non-competitive, or do not publish it.

## Required metadata

Every canonical production item must resolve:

- `claim_type`: `text | greek | history | interpretation | application`;
- `confidence`: `high | medium | contested`;
- `position`: `neutral | project`;
- `competitive`: boolean;
- `sources`: canonical source IDs.

Important project-position questions must also say this visibly in the user-facing wording or explanation.

## Disputed passages

For passages such as 1 Pet 3:19-20, 3:21, and 4:6:

1. state the exegetical problem neutrally;
2. record the major viable interpretations;
3. record the textual/grammatical arguments;
4. state the course position if one is adopted;
5. keep interpretation questions non-competitive;
6. permit competitive questions only for undisputed surrounding facts.

A disputed module should teach why the passage is difficult, not hide disagreement behind one commentator's answer.

## Old Testament / LXX intertext

Classify each relationship as one of:

- explicit quotation;
- clear verbal allusion;
- probable background;
- thematic parallel.

Do not call a proposed background an explicit quotation. Record both the 1 Peter passage and the OT/LXX source.

## Social history

Do not flatten ancient institutions into modern analogies. Example: `oiketai` in 1 Pet 2:18 concerns household slaves/servants in an ancient institution; an application to modern employment must be a separate application question, not the historical definition.

## Competitive standard

Default `competitive=false` for:

- application;
- genuinely disputed interpretation;
- authorship/date reconstruction;
- proposed allusion rather than explicit quotation;
- pastoral judgement scenarios;
- complex Greek claims before explicit source review.

Never enlarge ranking pools by weakening evidence standards.

## Copyright/source hygiene

- Store bibliographic metadata and source IDs, not copied commentary chapters.
- Paraphrase modern commentary unless a short quotation is necessary.
- Attribute ancient/public-domain sources accurately.

## Completion rule

A chapter is not complete because it has many questions. It is complete when its coverage matrix, source metadata, Greek review, historical review, disputed-passages review, tests, and exact-head CI/Security/CodeQL are complete.