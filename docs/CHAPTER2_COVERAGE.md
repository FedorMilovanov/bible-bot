# Chapter 2 Coverage Matrix

Status values: `done`, `partial`, `pending`, `n/a`.

A row is not complete until its evidence domains have been intentionally reviewed. A domain does not require a standalone question when the same claim is already adequately covered by an adjacent reviewed module; do not manufacture questions merely for matrix symmetry.

| Passage | Text | Greek | OT/LXX | History/social | Theology | Disputed | Application | Source review | Production |
|---|---|---|---|---|---|---|---|---|---|
| 2:1-3 | done | done | done (Ps 33/34) | n/a | done (growth) | partial (`logikon` remains lexical-review sensitive) | done | partial | draft only |
| 2:4-5 | done | done | partial (temple/priesthood background still merits final review) | n/a | done (spiritual house/priesthood) | n/a | done | partial | draft only |
| 2:6-8 | done | done | done (Isa 28; Ps 117/118; Isa 8) | n/a | done contextually (Christ/stone logic + 2:8 review) | done (appointment/accountability mapped) | done via 2:4-10 identity application | partial | draft only |
| 2:9-10 | done | done | done (Exod 19 + Hosea 1-2) | n/a | done at passage level (people-of-God synthesis) | n/a (whole-canon church/Israel system is intentionally not forced from one passage) | done | partial | draft only |
| 2:11-12 | done | done | n/a | done (social/spiritual exile distinction reviewed) | done contextually (identity and witness) | done (`day of visitation` mapped) | done | partial | draft only |
| 2:13-17 | done | done | n/a | partial (Roman authority/background review still to finish) | done (civil authority + canonical limit kept separate from direct text) | done at project-position level | done (2:15-16 freedom/good application) | partial | draft only |
| 2:18-20 | done | done | n/a | done (ancient household slavery/dependence named directly) | done contextually (2:21-25 supplies the Christological rationale) | n/a (historical/ethical cautions handled in history and application layers) | done with 2:18-25 suffering application | partial | draft only |
| 2:21-23 | done | done | done (Isa 53) | done (embodied suffering context reviewed) | done (Christ as example) | n/a | done | partial | draft only |
| 2:24-25 | done | done | done (Isa 53) | n/a | done (atonement + Shepherd/Overseer synthesis) | n/a (broader extent-of-atonement system is outside the claim made by this passage module) | done via 2:18-25 suffering application | partial | draft only |

## Current authoring structure

The chapter remains intentionally outside production. The repository contains a stable base draft plus supplemental reviewed modules, all guarded by tests.

Key reviewed modules include:

- `text*.py` — direct-text coverage through 2:25;
- `greek*.py` — Greek/morphology coverage through 2:25;
- `ot_psalm34.py`, `ot_stone.py`, `ot_exodus19.py`, `ot_hosea_2_10.py`, `ot_isaiah53.py`;
- `history_exiles_2_11.py`, `history_oiketai.py`, `history_bodily_suffering.py`;
- `theology_growth.py`, `theology_house.py`, `theology_civil.py`, `theology_people_text.py`, `theology_21_25.py`;
- `disputed_2_8.py`, `disputed_2_12.py`;
- `application_growth.py`, `application_identity.py`, `application_witness.py`, `application_freedom_2_15_16.py`, `application_suffering.py`;
- `tests/test_chapter2_draft_quality.py`, `tests/test_chapter2_supplemental_quality.py`, `tests/test_chapter2_new_review_modules.py`, `tests/test_chapter2_cross_layer_integrity.py`, and `tests/test_chapter2_intertext_exports.py`.

The chapter remains intentionally absent from `POOL_REGISTRY`. Do not expose a partial chapter in Telegram or Mini App merely to make progress visible.

## Remaining chapter-2 blockers

1. Finish the lexical review of `logikon` in 2:2 without collapsing lexical range into a preferred application.
2. Finish the temple/priesthood OT-background review for 2:4-5 without inventing an explicit quotation where there is only background/allusion.
3. Finish the Roman administrative/social-background review for 2:13-17 and keep later Pliny/Trajan evidence clearly labelled as later comparative evidence, not direct proof of 60s procedure.
4. Run a chapter-wide source audit: every source ID, URL, claim type, confidence, position, and competitive flag.
5. Run duplicate/wording/answer-leak review across base and supplemental modules.
6. Build one canonical chapter-2 aggregate only after the previous steps are green.
7. Add canonical pool/Telegram/Mini App exposure only after the complete chapter passes exact-head CI, Security Audit, CodeQL, and runtime smoke checks.

## Definition of complete chapter 2

Chapter 2 may enter production only after all rows are intentionally closed, all source IDs resolve, Greek evidence is verified, disputed/application items remain non-competitive, and fresh exact-head CI, Security Audit, and CodeQL succeed on the actual PR merge result.
