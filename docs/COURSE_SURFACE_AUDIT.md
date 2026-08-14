# Course Surface Audit

Branch target: `agent/course-surface-refactor`
Base: current `main` at audit time.

## Runtime map before refactor

```text
QUESTIONS / PRODUCT POOLS
  questions.POOL_REGISTRY + get_pool_by_key()
        |
        +--> questions.pool_policy.NON_SCORING_LEARNING_POOLS
        |
        +--> bot.py LEVEL_CONFIG --------------------------+
        |      | pool_key/name/count/points_per_q         |
        |      +--> legacy Telegram category/menu         |
        |      +--> legacy mode picker / deep links       |
        |      +--> Telegram production through legacy.*  |
        |                                                 |
        +--> web_api/quiz_start.py accepts pool_key ------+--> durable quiz session
        |                                                 |
        +--> miniapp/app.js LEVELS + HIST ----------------+
        |      +--> miniapp/chapter2.js hard-coded chapter2
        |      +--> miniapp/chapter3.js hard-coded chapter3
        |      +--> miniapp/index.html hard-coded cards
        |
        +--> result persistence
               +--> questions.pool_policy for ch2/ch3 non-scoring
               +--> database.POINTS_PER_QUESTION for scoring pools
```

## Duplicated authorities found

1. **Pool-to-course mapping** was duplicated between `bot.py` `LEVEL_CONFIG`, Mini App `LEVELS`/`HIST`, Chapter 2/3 scripts, and direct `/api/quiz/start` pool payloads.
2. **Availability/exposure** was duplicated. `questions.POOL_REGISTRY` already contained `chapter2` and `chapter3`; Mini App exposed both, while the legacy Telegram learning menu rendered Chapter 2 as `coming_soon` and had no Chapter 3 course entry.
3. **Scoring presentation** was duplicated. `bot.py` embedded `points_per_q`, Mini App embedded `pts` and timed multiplier copy, while result persistence separately owned the real non-scoring policy for Chapter 2/3.
4. **Question count** was duplicated as `10` in `LEVEL_CONFIG`, Mini App course arrays/scripts, and `web_api/quiz_start.py`.
5. **Mode eligibility** was client-presented independently. Mini App constructed modes locally and the API validated only generic mode names rather than the selected course policy.
6. **Callback trust boundary** was weak for course selection: Telegram level callbacks resolved configuration directly from callback-derived keys, and the Mini App could submit an arbitrary canonical `pool_key` if it existed.
7. **Home/course cards** were static HTML. A backend pool could exist without a matching card, and a card could outlive backend availability.
8. **Chapter stats naming/exposure** followed pool keys in history rather than a catalog label, so client copy and persisted stats labels could diverge.

## Existing authorities that must not change

- `questions/` remains the only question-source authority.
- `questions.POOL_REGISTRY` and `get_pool_by_key()` remain the canonical pool boundary.
- Chapter 2 remains learning-only and outside competitive/Challenge/Battle/random-all admission.
- Chapter 3 normal learning remains the complete reviewed bank and non-scoring.
- Chapter 3 competitive admission remains the explicit 12-card ranking authority in `questions.chapter3.ranking_authority`; the generic Chapter 3 learning pool must never become a competitive shortcut.
- Challenge and Battle selectors remain independent competitive entry points and are not generated from learning-course catalog entries.
- Mongo-backed session/result persistence remains authoritative for resume/cancel/finalization.

## Target map

```text
QUESTIONS AUTHORITY
  questions/*
      |
      v
POOL REGISTRY
  questions.POOL_REGISTRY / get_pool_by_key
      |
      +--> POOL POLICY (server scoring/ranking semantics)
      |
      v
COURSE CATALOG
  declarative metadata + computed availability
      |
      +--> public catalog serializer / API
      |       +--> Mini App cards + modes
      |
      +--> Telegram menu/callback/deep-link resolver
      |
      v
SERVER-AUTHORIZED QUIZ START
      |
      v
RESULT POLICY / DURABLE PERSISTENCE
```

## Refactor boundaries

- Introduce a small catalog module that stores metadata only and resolves pools lazily through the canonical registry.
- Public serialization omits question data, correct answers, source metadata, competitive allowlists, persistence IDs, and other server-only internals.
- Normal Mini App starts identify a **course**, not an arbitrary pool; the server resolves course -> pool and validates availability and allowed mode.
- Telegram callback/deep-link payloads identify a course/mode request only; the handler resolves it again against server state before sampling any questions.
- Missing pools fail closed and are filtered out of exposed catalog/menu output. A stale callback receives a controlled unavailable response.
- Challenge/Battle/random-all continue to use their existing explicit authorities and are covered by regression tests to ensure the learning catalog cannot widen them.

## Mini App integration findings

- Resume is server-backed through `/api/quiz/active`; preserve it.
- Cancel is owner-scoped and durable; preserve it.
- Chapter 2 and 3 previously loaded by independent scripts with duplicated modes/count/copy; those scripts are removed after refactor.
- Home cards previously were hard-coded in HTML; after refactor learning cards are rendered from `/api/catalog`.
- Retry/start state now retains `courseKey`; the server resolves the pool again instead of trusting a client pool authority.
- Catalog failure renders a retry state rather than clickable dead learning courses.
- Returning home, canceling, and visibility restoration refresh server catalog availability.
- Current-session conflict remains server-authoritative and cannot be bypassed by selecting another course card.

## Telegram integration findings

- Production composition imports legacy presentation, but durable lifecycle lives in `telegram_controller.py` and course navigation lives in `telegram_course_surface.py`.
- The narrow seam is menu/callback/deep-link resolution before `_launch_attempt`; Battle, Challenge, answer, timeout, resume, and cancel do not need rewrites.
- Existing deployed `level_*`, `confirm_level_*`, `intro_start_*`, and mode callbacks are compatibility aliases but are revalidated through catalog/policy.
- Unknown or stale course callbacks fail gracefully rather than raising `KeyError` or silently starting another pool.
- `/test` and `/start <course>` now enter `telegram_course_surface.py` directly. Production no longer reads or patches `bot.py LEVEL_CONFIG` or `bot.py choose_level`.
- Callback acknowledgement happens before durable course launch so an existing-session/database conflict does not leave the Telegram loading spinner active.

## Verification pass after the first implementation

A second audit was performed after the first green CI rather than assuming the initial refactor was complete. It found and corrected five important gaps:

1. **Stale callback bypass:** production still registered legacy `confirm_level_*` and `intro_start_*` handlers. Both now route through catalog validation; the production legacy allowlist forbids those handlers.
2. **Future chapter test trap:** tests/container smoke originally hard-coded Chapter 4/5 absence. They are now registry-driven, so a real Agent A/B backend registration surfaces automatically instead of failing CI merely because the chapter became available.
3. **Durable launch callback UX:** a valid course click could leave a Telegram spinner active when `_launch_attempt` reported an existing session or retryable DB conflict. The callback is now acknowledged before durable launch.
4. **Cross-surface learning persistence:** Mini App recorded Chapter 2/3 learning progress while Telegram only closed the session. Telegram now uses an idempotent per-attempt learning receipt and records the same progress-only counters without points, bonus, achievements, Challenge, or Battle mutation.
5. **Residual production LEVEL_CONFIG dependency:** `/start <course>` and `/test` still indirectly depended on the historical bot map through the controller/composition shadow. Production now resolves those paths in `telegram_course_surface.py`; unknown start tokens are stripped before the legacy controller fallback. The historical literal can remain transitional source without being a production authority.

The second audit also tightened catalog declaration validation so unknown mode IDs and callback-separator course keys fail at declaration time, and consolidated the legacy compatibility view so it is generated in one place by `course_catalog.py` rather than duplicated in the Telegram surface.

## Agent A/B integration check

The active Chapter 4 and Chapter 5 authoring Draft PRs were inspected as part of the second pass. Their backend registrations add canonical `chapter4`/`chapter5` pools and keep those reviewed banks out of random-all, Challenge, Battle, and generic competitive admission. The course-surface branch does not edit their question-bank files.

The only direct changed-file overlap is `questions/pool_policy.py`: the surface branch contains the richer unified policy registry and already declares Chapter 4/5 as learning-only. Integration must retain that unified policy behavior rather than replacing it with a narrower single-chapter set literal. This is a normal integration conflict, not a reason to duplicate UI policy.
