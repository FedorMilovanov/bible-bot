# Chapter 2 Coverage Matrix

Status values: `done`, `partial`, `pending`, `n/a`.

A row is not complete until its evidence domains have been intentionally reviewed. This file must be updated with chapter-2 content changes.

| Passage | Text | Greek | OT/LXX | History/social | Theology | Disputed | Application | Source review | Production |
|---|---|---|---|---|---|---|---|---|---|
| 2:1-3 | done | done | done (Ps 33/34) | n/a | done (growth) | partial (`logikon` remains lexical-review sensitive) | done | partial | draft only |
| 2:4-5 | done | done | partial (temple/priesthood background still merits final review) | n/a | done (spiritual house/priesthood) | n/a | done | partial | draft only |
| 2:6-8 | done | done | done (Isa 28; Ps 117/118; Isa 8) | n/a | partial | done (2:8 appointment/accountability mapped) | done via 2:4-10 identity application | partial | draft only |
| 2:9-10 | done | done | done (Exod 19 + Hosea 1-2) | n/a | partial (neutral people-of-God synthesis done; broader systematic boundary still pending) | partial (broader church/Israel system deliberately not forced from one text) | done | partial | draft only |
| 2:11-12 | done | done | n/a | done (social/spiritual exile distinction reviewed) | partial | done (`day of visitation` mapped) | done | partial | draft only |
| 2:13-17 | done | done | n/a | partial (Roman authority/background review still to finish) | done (civil authority + canonical limit kept separate from direct text) | done at project-position level | pending | partial | draft only |
| 2:18-20 | done | done | n/a | done (ancient household slavery/dependence named directly) | partial | partial | done with 2:18-25 suffering application | partial | draft only |
| 2:21-23 | done | done | done (Isa 53) | done/partial (embodied suffering context reviewed) | done (Christ as example) | n/a | done | partial | draft only |
| 2:24-25 | done | done | done (Isa 53) | n/a | done (atonement + Shepherd/Overseer synthesis) | partial (systematic extent questions remain outside direct text) | done via 2:18-25 suffering application | partial | draft only |

## Current authoring structure

The chapter remains intentionally outside production. The repository now contains two reviewed layers:

1. `questions/chapter2/draft.py` — the stable base authoring aggregate for direct text, Greek, OT/LXX, and already-integrated theology.
2. Supplemental review modules protected by dedicated tests, including social history, application, civil-authority theology, people-of-God synthesis, and disputed-passage labs.

Key reviewed modules now include:

- `text*.py` — direct-text coverage through 2:25;
- `greek*.py` — Greek/morphology coverage through 2:25;
- `ot_psalm34.py`, `ot_stone.py`, `ot_exodus19.py`, `ot_hosea_2_10.py`, `ot_isaiah53.py`;
- `history_exiles_2_11.py`, `history_oiketai.py`, `history_bodily_suffering.py`;
- `theology_growth.py`, `theology_house.py`, `theology_civil.py`, `theology_people_text.py`, `theology_21_25.py`;
- `disputed_2_8.py`, `disputed_2_12.py`;
- `application_growth.py`, `application_identity.py`, `application_witness.py`, `application_suffering.py`;
- `tests/test_chapter2_supplemental_quality.py` and `tests/test_chapter2_new_review_modules.py`.

The chapter remains intentionally absent from `POOL_REGISTRY`. Do not expose a partial chapter in Telegram or Mini App merely to make progress visible.

## Remaining chapter-2 blockers

1. Add a separate non-competitive application module for 2:13-17 without turning present-day political judgment into a factual quiz.
2. Finish the Roman administrative/social-background review for 2:13-17 and keep it distinct from theology.
3. Decide whether 2:18-20 needs one additional theology card beyond the already reviewed text/history/application layers; do not add one merely for symmetry.
4. Finish the broader systematic boundary around 2:9-10 only if it can be stated without making one passage carry the whole church/Israel system.
5. Run a chapter-wide source audit: every source ID, URL, claim type, confidence, position, and competitive flag.
6. Run duplicate/wording/answer-leak review across base and supplemental modules.
7. Build one canonical chapter-2 aggregate only after the previous steps are green.
8. Add canonical pool/Telegram/Mini App exposure only after the complete chapter passes exact-head CI, Security Audit, CodeQL, and runtime smoke checks.

## Definition of complete chapter 2

Chapter 2 may enter production only after all rows are intentionally closed, all source IDs resolve, Greek evidence is verified, disputed/application items remain non-competitive, and fresh exact-head CI, Security Audit, and CodeQL succeed on the actual PR merge result.
