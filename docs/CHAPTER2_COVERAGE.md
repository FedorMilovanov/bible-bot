# Chapter 2 Coverage Matrix

Status values: `done`, `partial`, `pending`, `n/a`.

A row is not complete until its evidence domains have been intentionally reviewed. This file must be updated with chapter-2 content changes.

| Passage | Text | Greek | OT/LXX | History/social | Theology | Disputed | Application | Source review | Production |
|---|---|---|---|---|---|---|---|---|---|
| 2:1-3 | done | done | done (Ps 33/34) | n/a | pending | partial (`logikon`) | pending | partial | draft only |
| 2:4-5 | done | done | partial (temple/priesthood background still to review) | n/a | pending | n/a | pending | partial | draft only |
| 2:6-8 | done | done | done (Isa 28; Ps 117/118; Isa 8) | n/a | pending | pending (2:8 appointment/accountability) | pending | partial | draft only |
| 2:9-10 | done | done | partial (Exod 19 done; Hosea 1-2 still missing) | n/a | pending | pending (people-of-God synthesis) | pending | partial | draft only |
| 2:11-12 | done | done | n/a | pending | pending | pending (`day of visitation`) | pending | partial | draft only |
| 2:13-17 | partial (2:16-17 done; 2:13-15 missing) | done | n/a | pending | pending | pending (scope of civil submission) | pending | partial | draft only |
| 2:18-20 | done | done | n/a | pending (ancient household slavery) | pending | pending | pending | partial | draft only |
| 2:21-23 | done | done | done (Isa 53) | pending | partial (Christ as example covered) | pending | pending | partial | draft only |
| 2:24-25 | done | done | done (Isa 53) | n/a | done (atonement + Shepherd/Overseer synthesis) | partial (systematic extent questions remain outside direct text) | pending | partial | draft only |

## Current authoring inventory

Current assembled draft: **56 questions**, intentionally outside production.

- Direct text: **27** questions.
- Greek: **17** questions, all source-backed by SBLGNT/MorphGNT and non-competitive while in draft review.
- OT/LXX: **8** questions covering Psalm 33/34, the stone chain, Exodus 19, and Isaiah 53.
- Conservative theology: **4** questions for 2:21-25, with project positions visibly labelled and non-competitive.

Key files:

- `questions/chapter2/draft.py` — single authoring aggregate.
- `questions/chapter2/text*.py` — direct-text slices.
- `questions/chapter2/greek*.py` — Greek/morphology slices.
- `questions/chapter2/ot_*.py` — independently reviewed OT/LXX clusters.
- `questions/chapter2/theology_21_25.py` — conservative atonement/Christology synthesis.
- `questions/chapter2/sources.py` and `sources_11_25.py` — reviewed source metadata.
- `questions/source_registry.py` — collision-safe aggregation of legacy and new chapter sources.

The chapter remains intentionally absent from `POOL_REGISTRY`. Do not expose a partial chapter in Telegram or Mini App merely to make progress visible.

## Remaining chapter-2 blockers

1. Add direct-text coverage for 2:13-15 without mixing it with a full political-theology conclusion.
2. Add Hosea 1-2 background for 2:10.
3. Complete theology for 2:1-10, especially spiritual growth, living stones, priesthood, and corporate mission.
4. Add a disputed module for 2:8 that distinguishes the direct syntax from broader doctrines of divine appointment and human responsibility.
5. Add a carefully sourced civil-authority module for 2:13-17; direct command, canonical limits, and modern application must remain distinct.
6. Add social-history treatment for 2:18-20 that names ancient household slavery/dependence accurately rather than redefining `oiketai` as ordinary modern employment.
7. Add application questions as a separate non-competitive domain.
8. Expand source-truth regression tests over the full draft aggregate.
9. Re-run the full source/commentary review before canonical promotion.

## Definition of complete chapter 2

Chapter 2 may enter production only after all rows are intentionally closed, all source IDs resolve, Greek evidence is verified, disputed/application items remain non-competitive, and fresh exact-head CI, Security Audit, and CodeQL succeed on the actual PR merge result.