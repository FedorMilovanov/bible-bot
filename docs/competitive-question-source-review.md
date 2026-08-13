# Canonical question source review and main admission

Reviewed: 2026-08-13

This document records the final content-truth model used by production before integration into `main`.

## Canonical authority

`questions/chapter1.py` and `questions/intro.py` remain the historical authoring corpus. Production code does not consume them directly. The public `questions` package builds canonical copies through the content-truth/review layers, and a regression guard rejects production imports that bypass that package boundary.

Every canonical item carries explicit epistemic metadata:

- `claim_type`: `text`, `greek`, `history`, `interpretation`, or `application`;
- `confidence`: `high`, `medium`, or `contested`;
- `position`: `neutral` or `project`;
- `competitive`: whether the item may affect PvP/Challenge ranking;
- `sources`: source IDs resolved through the canonical source catalog.

Project/traditional positions remain available for learning but are visibly labelled rather than presented as universal scholarly consensus.

## Confirmed P0 repairs

The canonical runtime bank repairs all four confirmed source-text failures found in the marathon audit:

- `geo_04` — Ephesus is present and is the correct answer; the old item named Ephesus only in its explanation while marking Rome correct.
- `ling2_12` — uses the real 1 Pet 1:6 sequence `ἐν ᾧ ἀγαλλιᾶσθε, ὀλίγον ἄρτι...`; the malformed `ἐν ὀλίγον ἄρτι` construction is not used.
- `ling2_15` — no invented `ἐν` before `φθαρτοῖς`; the item tests the actual dative forms in 1 Pet 1:18.
- `ling3_06` — uses `ἀγαπήσατε`, aorist active imperative 2nd plural, rather than the erroneous present-imperative form.

Dedicated regression tests protect these repairs.

## Full Greek review

All three Greek learning pools were reviewed item by item against the Greek text/morphology. The canonical review additionally corrects or narrows claims involving:

- `πρόγνωσις` and theological over-reading;
- `διασπορά` versus the corrupted `σπορά` gloss;
- `ἀναγεννήσας` and aorist-aspect overstatement;
- `εἰ δέον` without an invented `ἐστίν`;
- `ψυχή` in `σωτηρίαν ψυχῶν`;
- proposed Exodus background for 1 Pet 1:13 as an allusion/background rather than an explicit citation;
- the syntax around `λόγου`, `ζῶντος`, and `μένοντος` in 1 Pet 1:23;
- the future horizon of salvation in 1 Pet 1:5;
- election/calling as theological synthesis rather than morphology;
- `ἀμώμου`, `ἀσπίλου`, `ἐξηράνθη`, `ἀνυπόκριτος`, `ἐκτενῶς`, `φιλαδελφία`, and `ἀγάπη` without folk-etymological or one-gloss overclaiming.

Greek learning items remain non-competitive unless they pass an explicit source-review release.

## Nero and historical review

Nero questions distinguish source layers instead of collapsing them into one narrative:

- Tacitus, *Annals* 15.44 is used for Nero blaming Christians after the fire and for the specific punishments he lists;
- Suetonius is named for claims drawn from *Nero* 38/49, including the fire-duration report and `Qualis artifex pereo!`;
- Eusebius, *Church History* 2.25 is identified as a later church-historical witness for traditions concerning Peter and Paul under Nero;
- Paul’s Roman citizenship is tested as the direct Acts claim rather than as an automatically proven legal cause of one precise execution method;
- Neronian Rome is treated as historical context whose connection to 1 Peter depends on the disputed dating of the letter.

## Geography review

The address in 1 Pet 1:1 is described as five geographic/provincial names — Pontus, Galatia, Cappadocia, Asia, and Bithynia — rather than flattening every name into an identical administrative category. `διασπορά` is kept at its basic sense of dispersion/diaspora before further theological interpretation.

## Intro/authorship review

The course retains its traditional Petrine position while clearly identifying it as the course position. Rewritten introduction items separate:

- internal textual claims from historical proof;
- early patristic reception from modern authorship adjudication;
- the high-Greek objection from the stronger claim that Petrine authorship is impossible;
- knowledge of Peter’s death from the stronger claim that pseudonymity is logically impossible;
- Silvanus named in 1 Pet 5:12 from disputed secretary/editor/courier reconstructions;
- dating, Babylon/Rome, and arguments from silence from uncontested facts.

## Competitive integrity

Learning availability is broader than ranking eligibility. PvP and Challenge use the canonical ranking policy rather than the broad casual/random learning bank. Contested interpretation/application material does not decide another user’s competitive result unless an item has received an explicit source-reviewed ranking release.

The currently explicit source-reviewed ranking releases are:

- `easy_12` — conditional imperial chronology;
- `med_02` — Tacitus on Nero and Christians after the fire;
- `hard_02` — lexical sense of `παρακύπτω`;
- `hard_12` — the three Greek adjectives in 1 Pet 1:4.

## Mini App lifecycle correctness

The Mini App remains server-authoritative for quiz timing. On WebView/browser resume (`visibilitychange` / `pageshow`), an active answerable question is reloaded from the server so the UI’s remaining time is resynchronised with the authoritative server timestamp instead of trusting throttled client intervals. JS unit tests cover the visible/hidden and answerable/pending cases.

## Main admission gate

This content/runtime state may enter `main` only after a fresh pull-request run against the current `main` base succeeds for all admission workflows:

- CI: actionlint, dependency/secret guards, Ruff, compile, full pytest, Mini App JavaScript syntax/unit tests, Docker build, production imports, and web-container smoke;
- Security Audit;
- CodeQL for Python and JavaScript/TypeScript.

Old successful or failed runs from a previous PR base are not accepted as evidence for the final `main` merge.

## Key sources

- SBL Greek New Testament / MorphGNT morphology for 1 Peter.
- Tacitus, *Annals* 15.44.
- Suetonius, *Nero* 38 and 49.
- Eusebius, *Church History* 2.25.
- Pliny / Trajan, *Letters* 10.96–97.
- Oxford Handbook of 1 Peter, contested issues in authorship/date/place.
- E. Randolph Richards, “Silvanus Was Not Peter’s Secretary,” JETS 43.3 (2000).
- Cambridge/New Testament Studies work on Acts 4:13 and literacy terminology.
