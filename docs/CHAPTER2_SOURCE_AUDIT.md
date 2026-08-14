# Chapter 2 Source Audit

Audited: 2026-08-14.

This document records the manual source-quality pass for the 1 Peter chapter-2 authoring bank. It supplements automated source-ID resolution tests; it does not replace them.

## Audit method

For each load-bearing source used by chapter 2:

1. resolve the source ID through `questions/source_registry.py`;
2. verify that the referenced page, indexed result, or document exists;
3. verify that its title/content corresponds to the metadata in the catalog;
4. distinguish primary evidence, academic control, conservative exposition, translation/exegesis, and project doctrinal control;
5. record chronological or interpretive limits that matter to the question using the source;
6. do not make CI depend on live network availability.

A transient fetch failure is not enough to call a source dead. Search/index confirmation may establish that the page still exists when a site blocks automated fetching.

## Verified source groups

### Text and Greek

- `sblgnt` — primary Greek surface text.
- `morphgnt_1peter` — morphology/parsing control.
- `septuagint_bible` — LXX text access for OT/LXX comparison.

These remain the primary controls for text and machine-checkable morphology. Commentary must not override them.

### Conservative exposition and project theology

The following source targets were confirmed as real and relevant to their declared use:

- GTY material for 1 Peter 2:1-3, 2:4-10, 2:6-8, 2:18-21, 2:21-23, and 2:24-25;
- GTY civil-disobedience/application material used only at the interpretation/application layer;
- `tmsj_felix_penal_substitution` — Paul W. Felix, TMSJ 20.2 (2009), now linked directly to the article PDF rather than only to the journal archive;
- `tms_doctrinal_statement` — project doctrinal control, not a neutral lexical/history source.

GTY study-guide URLs may occasionally fail an automated direct fetch while the exact GTY article/sermon remains indexed and available. Such cases were cross-checked by exact title/series number instead of being silently removed.

### Independent academic / historical controls

Verified controls include:

- Cambridge NTS, `cambridge_missing_masters`;
- Cambridge NTS, `cambridge_following_footsteps`;
- Cambridge/New Cambridge Bible Commentary, `cambridge_reese_1peter`;
- Oxford, `oxford_kantor_pontus_bithynia`;
- HTS, `hts_ancient_slavery`;
- SBJT, `sbjt_parker_1p2_4_10`;
- Thomas Schreiner NAC bibliographic control, `schreiner_nac_1peter`;
- Naseri 1 Peter 2:11 study as a supplementary specialist source, no longer the sole independent control for the social-identity claim.

### Translation / disputed-exegesis control

- `ubs_handbook_1p2_12` was confirmed through the indexed TIPs/UBS translation-commentary entry. The direct site may present an anti-bot/interstitial page to automated fetchers; that is a transport limitation, not evidence that the source citation is fabricated.

### Roman historical comparison

- `pliny_trajan_10_96_97` is real primary evidence for Pliny's governorship and correspondence with Trajan in Bithynia-Pontus.
- It is **later comparative evidence**, from the early second century, not direct evidence for the exact administrative procedure in the likely first-century setting of 1 Peter.
- `oxford_kantor_pontus_bithynia` is the modern control used to prevent anachronistic projection of Pliny's later situation back into 1 Peter 2:13-14.

The Roman history card must remain `claim_type=history`, `position=neutral`, `competitive=false`.

## Important audit finding: atonement scope

`ch2_theol_002` correctly aims to teach substitutionary sin-bearing in 1 Peter 2:24, but its current authoring explanation contains wording narrower than the proposition actually being tested.

The source set used by that card agrees on substitutionary sin-bearing but does not use one identical formulation for the broader systematic question of the extent of the atonement. Therefore:

- the question may teach substitutionary sin-bearing;
- it must not use this card to settle the broader extent-of-atonement debate;
- until the wording is successfully narrowed to that reviewed scope, **`ch2_theol_002` is quarantined from canonical production promotion**;
- do not solve this by deleting one side of the source evidence or by weakening the project-position rules.

This quarantine does not create a chapter-2 coverage gap: direct-text questions and the other 2:21-25 theology/intertext modules already establish that Christ bears sins and that the passage is read substitutionally by the course.

## Source-quality conclusions

- No chapter-2 question may be promoted merely because its source ID resolves in Python.
- Primary text/morphology, history, commentary, and project theology remain separate evidence classes.
- Later historical analogies must stay visibly later.
- A source page being reachable does not prove that the source is fit for the claim.
- A source page temporarily resisting automated fetching does not by itself prove that the source is dead.
- Weak or narrow specialist sources should be supplemented by stronger academic controls when available.
- Disagreement between otherwise approved sources must narrow or quarantine the claim; it must not be hidden.

## Promotion status

The chapter-2 source set is reviewed for authoring use. Canonical production promotion still requires:

1. keeping `ch2_theol_002` quarantined until its scope wording is corrected;
2. final cross-layer schema/duplicate review on the canonical aggregate;
3. exact-head CI, Security Audit, and CodeQL after the canonical aggregate is built;
4. separate production-pool/UI admission after the chapter-level definition of done is satisfied.
