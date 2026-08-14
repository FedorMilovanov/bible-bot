# 1 Peter Chapter 3 — normal-learning product admission

**Status:** `NORMAL LEARNING WIRED / NON-SCORING / RANKING CLOSED`

This product layer is stacked on the exact-green reviewed-bank checkpoint:

`fe0b9ea7ed0098c2a625bb6956df0a1368e182ef`

The reviewed parent already established a 165-card deep-copy bank with explicit source, metadata, domain-coverage, project-label, and ranking-candidate boundaries.

## Product admission

Chapter 3 is admitted to the root question registry only through:

```text
CHAPTER3_REVIEWED_QUESTIONS -> chapter3_questions -> POOL_REGISTRY["chapter3"]
```

Raw lane pools and the staging aggregate do not cross the root product boundary directly.

The Mini App exposes a dedicated Chapter-3 learning card and starts normal quizzes with:

```text
startQuiz("chapter3", mode, 10, false)
```

The existing server remains authoritative for question selection, answer checking, and result finalization.

## Learning is not scoring

`questions.pool_policy.NON_SCORING_LEARNING_POOLS` now contains both `chapter2` and `chapter3`.

The existing `web_api.result_store.apply_regular_result_once()` therefore routes Chapter-3 completions through the learning-only persistence path.

A Chapter-3 completion may update only course-progress fields such as:

```text
chapter3_attempts
chapter3_correct
chapter3_total
chapter3_best_score
```

Its receipt is required to remain:

```text
points = 0
daily_bonus = 0
new_achievements = []
kind = learning
```

It must not increment ranking aggregates such as `total_points`, `total_tests`, or `perfect_count`.

## Ranking containment

Normal-learning admission does not modify the competitive composition.

Chapter-3 IDs remain absent from:

- `random_all`;
- `COMPETITIVE_POOL`;
- `BATTLE_POOL`;
- every `CHALLENGE_POOLS` pool.

This applies even to reviewed objective cards that retain `competitive=True` as future candidate metadata.

```text
competitive=True ON REVIEWED CARD != RANKING ADMITTED
chapter3 normal learning != ranking
chapter3 normal learning != Challenge
chapter3 normal learning != Battle
chapter3 normal learning != random_all
```

A later ranking wave must make an explicit card-by-card source-review decision and change root ranking composition deliberately.

## Mini App presentation

The home screen now exposes:

`📙 Глава 3 — Reviewed-курс · спорные места · без рейтинга`

The Chapter-3 course screen states that disputed interpretations and project positions are marked and not used for ranking. The reviewed bank itself visibly prefixes project-position questions with `[Позиция курса]`.

Three normal learning modes are available, matching Chapter 2:

- relaxed — no timer;
- timed — 30 seconds;
- speed — 15 seconds.

Each requests 10 server-selected Chapter-3 questions.

## Legacy Telegram menu

This layer intentionally does **not** rewrite the monolithic legacy `bot.py` just to add another menu button. Replacing that large file wholesale would be a fragile infrastructure workaround, not a quality integration.

The Chapter-3 supported learning surface in this admission step is the Mini App/API. A future Telegram-menu refactor should expose reviewed learning pools through a small modular registry rather than a whole-file rewrite.

## Exact product gate

This branch is not considered product-green until fresh exact-head:

- CI;
- Security Audit;
- CodeQL Stacked PR

all succeed after the root pool, non-scoring policy, Mini App entry, and product-contract tests are present.

Even after those checks pass, the stacked PR remains Draft/unmerged and `main` remains untouched.
