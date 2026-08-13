# Chapter 2 Coverage Matrix

Status values: `done`, `partial`, `pending`, `n/a`.

| Passage | Text | Greek | OT/LXX | History/social | Theology | Disputed | Application | Source review | Product state |
|---|---|---|---|---|---|---|---|---|---|
| 2:1-3 | done | done | done | n/a | done | done | done | done | canonical learning |
| 2:4-5 | done | done | done | n/a | done | n/a | done | done | canonical learning |
| 2:6-8 | done | done | done | n/a | done | done | done | done | canonical learning |
| 2:9-10 | done | done | done | n/a | partial: one editorial quarantine | n/a | done | done | canonical minus quarantine |
| 2:11-12 | done | done | n/a | partial: one editorial quarantine | done | done | done | done | canonical minus quarantine |
| 2:13-17 | done | done | n/a | done | done | done | done | done | canonical learning |
| 2:18-20 | done | done | n/a | partial: one editorial quarantine | done | n/a | done | done | canonical minus quarantine |
| 2:21-23 | done | done | done | partial: one editorial quarantine | done | n/a | done | done | canonical minus quarantine |
| 2:24-25 | done | done | done | n/a | done | n/a | done | done | canonical learning |

## Canonical learning boundary

- `questions/chapter2/draft.py` is authoring/evidence input.
- `questions/chapter2/reviewed.py` is the Chapter-2 promotion boundary.
- `questions.POOL_REGISTRY["chapter2"]` must equal the reviewed bank.
- `questions/source_registry.py` is the merged source registry exported by `questions.SOURCE_CATALOG`.
- `questions/pool_policy.py` marks `chapter2` as learning-only for scoring purposes.
- Chapter 2 is not in competitive, battle, Challenge, or legacy random pools.
- Mini App uses the canonical `chapter2` key.
- Telegram finalization supports the learning-only policy, but Telegram menu admission is still pending.

## Editorial state

The Greek pass and the application pass are complete. Roman-context and civil-authority cards have also received the parallel-alternative pass.

Current editorial quarantine:

- `ch2_hist_001`
- `ch2_hist_003`
- `ch2_hist_004`
- `ch2_theol_010`

These IDs remain in authoring modules but are excluded from the reviewed/canonical bank until rewritten and re-reviewed.

`ch2_theol_002` is admitted after its proposition was narrowed to the supported 2:24 claim; it does not decide a broader systematic question.

## Remaining blockers

1. Rewrite or intentionally retire the four quarantined cards.
2. Add the Telegram menu entry through the canonical `chapter2` pool; do not duplicate question data in client code.
3. Add a Telegram integration contract for the new entry and learning-only finalization.
4. Keep ranking admission separate from normal learning admission.
5. Run fresh exact-head CI, Security Audit, CodeQL, production import, and web/runtime smoke after the final integration tree.

## Definition of complete Chapter 2

Chapter 2 is complete only when the remaining editorial quarantine is resolved, Telegram and Mini App both consume the same canonical learning pool, ranked paths remain isolated, and all required exact-head gates are green.
