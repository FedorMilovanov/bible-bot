# Chapter 2 Coverage Matrix

Status values: `done`, `partial`, `pending`, `n/a`.

A row is not complete until its evidence domains have been intentionally reviewed. This file must be updated with chapter-2 content changes.

| Passage | Text | Greek | OT/LXX | History/social | Theology | Disputed | Application | Source review | Production |
|---|---|---|---|---|---|---|---|---|---|
| 2:1-3 | done | done | partial | n/a | pending | partial (`logikon`) | pending | partial | draft only |
| 2:4-5 | done | done | partial | pending | pending | n/a | pending | partial | draft only |
| 2:6-8 | done | done | pending | n/a | pending | pending (2:8 appointment/accountability) | pending | partial | draft only |
| 2:9-10 | done | done | pending | n/a | pending | pending (people-of-God synthesis) | pending | partial | draft only |
| 2:11-12 | pending | pending | pending | pending | pending | pending | pending | pending | not built |
| 2:13-17 | pending | pending | pending | pending | pending | pending (scope of civil submission) | pending | pending | not built |
| 2:18-20 | pending | pending | pending | pending (household slavery) | pending | pending | pending | pending | not built |
| 2:21-23 | pending | pending | pending (Isa 53) | pending | pending | pending | pending | pending | not built |
| 2:24-25 | pending | pending | pending (Isa 53) | n/a | pending (atonement) | pending (extent/systematic synthesis if asked) | pending | pending | not built |

## Current authoring inventory

- `questions/chapter2/text.py`: direct-text slice for 2:1-10.
- `questions/chapter2/greek.py`: source-backed Greek slice for 2:1-10.
- `questions/chapter2/sources.py`: reviewed chapter-specific sources.
- `questions/source_registry.py`: collision-safe aggregation of legacy + chapter source catalogs.

The chapter remains intentionally absent from `POOL_REGISTRY`. Do not expose a partial chapter in Telegram/Mini App merely to make progress visible.

## Required source-review focus before 2:1-10 can be marked complete

1. `logikon adolon gala` — distinguish lexical range from the course application to Scripture.
2. Psalm 33:9 LXX / Psalm 34:8 numbering and wording behind 2:3.
3. Isa 28:16, Psalm 117:22 LXX / 118:22 MT, Isa 8:14 stone cluster.
4. Exod 19:5-6 and the covenant vocabulary of 2:9.
5. Hosea 1-2 background of 2:10.
6. Relationship of divine appointment and human disobedience in 2:8 must be presented as theological/exegetical synthesis, not as a simplistic lexical fact.
7. Church/Israel continuity conclusions must be labelled at the level actually supported by the question.

## Definition of complete chapter 2

Chapter 2 may enter production only after all rows are complete, all source IDs resolve, Greek evidence is verified, disputed/application items are non-competitive, and fresh exact-head CI/Security/CodeQL succeeds.