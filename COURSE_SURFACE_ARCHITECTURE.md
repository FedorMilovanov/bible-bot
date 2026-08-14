# Course Surface Architecture

## Purpose

Learning-course exposure is a product integration concern, not a question-bank or scoring concern. The application has one declarative course catalog consumed by Telegram and the Mini App, while `questions/` remains the sole question authority and server-side pool policy remains the scoring authority.

```text
QUESTIONS AUTHORITY
  questions/*
      ↓
POOL REGISTRY
  questions.POOL_REGISTRY / get_pool_by_key()
      ↓
COURSE CATALOG
  course_catalog.py
      ↓
TELEGRAM / MINI APP
  telegram_course_surface.py / GET /api/catalog
      ↓
SERVER QUIZ SESSION
  telegram_controller.py / web_api.quiz_start
      ↓
RESULT POLICY + DURABLE PERSISTENCE
  questions.pool_policy + legacy/web result stores
```

## Authority boundaries

### Catalog != question source authority

`course_catalog.py` owns only product-facing course metadata and the mapping from a course key to a canonical pool key. It does not import chapter question lists and it cannot make a question eligible for Challenge, Battle, or ranking.

The actual questions remain behind `questions.POOL_REGISTRY` and `questions.get_pool_by_key()`. A catalog entry whose canonical pool does not exist, has fewer questions than its declared default count, or has no registered pool policy is unavailable and cannot start.

Course declarations also fail fast on malformed callback keys and unknown interaction modes; a declaration cannot invent a new `ranked` mode and rely on a client to interpret it.

### UI != scoring authority

The catalog does not independently invent score values. A course resolves to a pool, then `questions.pool_policy` supplies the pool's normal-learning scoring semantics. Public UI metadata is serialized from that server policy so Telegram and the Mini App can describe the experience without deciding it.

The Mini App cannot submit `ranked`, `scoring_mode`, `points_per_question`, or `score_multiplier` as a normal-course override. Normal quiz start identifies a `course_key`; the server resolves course -> pool -> policy and derives count/mode behavior itself.

Public mode metadata contains only mode identity, label, timer description, and time limit. Score multipliers remain server-only so a learning-only course never receives misleading `x1.5`/`x2` scoring presentation and the browser cannot treat a multiplier as authority.

Existing scored persistence remains authoritative. A regression contract requires every scored Telegram catalog pool to have the same points value in the legacy persistence map, while learning-only pools must be absent from that map.

Learning-only Telegram results use an idempotent per-attempt learning receipt. They update only `<chapter>_attempts`, `<chapter>_correct`, `<chapter>_total`, and `<chapter>_best_score`; they do not increment competitive totals, daily bonus, achievements, Challenge, or Battle state. A retry is accepted only if the stored receipt still matches the same course, score, total, and zero-scoring side effects. Any mismatch fails closed instead of double-counting or silently accepting corrupted state. Mini App learning results follow the same progress-only product policy.

Public profile filtering derives course progress field names from the course catalog (plus legacy level keys), so newly registered Chapter 4/5/6 progress does not require a second hard-coded API allowlist. Internal learning receipts are never serialized to the client.

### Callback / deep link != authorization

Telegram callback data and `/start <course>` tokens name a requested course/mode only. `telegram_course_surface.py` re-resolves all of the following immediately before launch or course rendering:

- course key exists;
- course is exposed on Telegram;
- requested mode is allowed by that course;
- canonical pool exists;
- canonical pool has enough questions;
- pool has a registered product policy.

Only after those checks does the handler sample questions and call the durable Telegram session launcher. Unknown, malformed, unavailable, or stale course requests fail closed instead of raising, falling back to another pool, or enabling ranking.

Previously delivered `level_*`, `confirm_level_*`, `intro_start_*`, and legacy mode callback payloads are compatibility aliases only; they pass through the same catalog resolver. They do not carry a ranking switch. Callback clicks are acknowledged before durable launch so active-session/database conflicts cannot leave a Telegram spinner hanging.

## Catalog shape

A `CourseEntry` declares presentation and navigation metadata:

- stable course key;
- title and description;
- canonical pool key;
- default question count;
- group/order/icon;
- eligible surfaces;
- allowed interaction modes;
- optional legacy callback alias during migration.

Scoring/ranking semantics are deliberately not independent mutable fields on the entry. They are read from the canonical pool policy.

The public Mini App serializer returns only client-useful presentation/policy description. It does not expose canonical pool keys, correct answers, source/evidence internals, competitive allowlists, ranking authorization IDs, persistence IDs, session IDs, or score multipliers.

`GET /api/catalog` is sent with `Cache-Control: no-store, max-age=0` so availability after a deployment is not hidden behind a stale browser/proxy catalog.

## Chapter 2

Chapter 2 is exposed through the canonical `chapter2` course on Telegram and the Mini App when the reviewed `chapter2` pool exists and satisfies the catalog contract.

Its pool policy is learning-only:

- normal learning only;
- non-ranked;
- zero competitive/base points;
- default 10-question course sessions;
- learning progress persists without competitive totals;
- no admission into Challenge/Battle/random-all is created by the catalog.

## Chapter 3

Chapter 3 normal learning resolves to the full canonical reviewed `chapter3` pool. Its normal course policy is learning-only and non-ranked, with the same progress-only persistence semantics as Chapter 2.

Competitive Chapter 3 remains a separate authority in `questions.chapter3.ranking_authority` and the existing resolved 12-card competitive pool. The course catalog never converts the full reviewed Chapter 3 pool into a competitive pool. Challenge and Battle continue to consume their explicit competitive authorities, independent of course navigation.

## Future Chapters 4/5 and later chapters

Chapter 4 and Chapter 5 course declarations and learning policies are present, but each declaration remains invisible and unstartable while its canonical pool is absent. This is intentional fail-closed behavior: no dead buttons are emitted.

Tests and container smoke are registry-driven rather than hard-coding Chapter 4/5 absence. Therefore when Agent A/B registers a canonical `chapter4` or `chapter5` pool in `questions.POOL_REGISTRY`, the corresponding declaration becomes available to both course surfaces on the next deployment without changing Telegram handlers, Mini App JavaScript, profile serialization, or the tests merely to permit exposure.

For a later Chapter 6, the expected product integration is:

1. question owners register the canonical backend pool;
2. product integration adds one declarative course entry and its pool policy;
3. Telegram, Mini App, and public chapter-progress serialization consume it automatically through the existing catalog boundary.

No chapter-specific Mini App script, Telegram menu-copy block, duplicated score constant, or profile-field allowlist entry is required.

## Mini App behavior

The home learning menu is rendered from `/api/catalog`. The static HTML contains only a course-menu container; Chapter 2/3-specific scripts have been removed.

Normal starts send `course_key`, mode, and the server-declared count. The server verifies all of them. A previously deployed Mini App bundle that still sends a `pool_key` is accepted only when that pool maps unambiguously to one currently exposed Mini App course; arbitrary pools such as `competitive_all` cannot use this compatibility path.

Durable active-session resume remains higher priority than catalog rendering, so an in-progress session can recover even if a fresh catalog request is temporarily unavailable. Cancel remains server-backed. Catalog fetch failures show a retry control rather than clickable stale learning cards. Returning home, canceling, and browser visibility restoration refresh availability.

Learning-only result copy is descriptive; scoring and persistence decisions stay server-owned. Challenge and Battle remain separate product entry points and are not generated from the learning catalog.

## Telegram behavior

`telegram_production.py` is the only production composition root. `/test`, `/start <course>`, course groups, course cards, modes, stale callbacks, and course launch all enter `telegram_course_surface.py` directly.

The historical `bot.py` `LEVEL_CONFIG`, `choose_level`, `chapter_1_menu`, and `historical_menu` definitions remain transitional standalone source code because the monolith is intentionally not being rewritten wholesale. **Production neither reads nor monkey-patches that course map.** Unknown `/start` tokens are stripped before falling through to `telegram_controller.start`, so its legacy argument branch cannot become a production course authority.

Resume, cancel, Challenge, Battle, answer delivery, timeout handling, and durable finalization remain owned by their existing controllers. The refactor changes only the learning-menu/course-start/result-policy seam needed for consistent course exposure.

## Regression invariants

Tests enforce that:

- catalog keys are unique and order is deterministic;
- course keys cannot smuggle callback separators and modes come from the known mode set;
- missing pools fail closed;
- Chapter 2/3 appear on both learning surfaces;
- Chapter 2/3 normal courses are learning-only/non-ranked;
- Chapter 3 competitive authority remains exactly its authorized 12-card subset;
- future Chapter 4/5 exposure tracks the canonical backend registry rather than a hard-coded UI/test allowlist;
- public catalog does not leak question/competitive internals or score multipliers;
- public profile output exposes catalog chapter progress without exposing learning receipts;
- normal Mini App start rejects client ranking/scoring overrides and arbitrary pools;
- malformed/stale Telegram callbacks and deep links fail gracefully;
- production never references `legacy.LEVEL_CONFIG` or `legacy.choose_level` for course navigation;
- Telegram learning progress is idempotent, validates retry receipts, and cannot mutate competitive totals;
- the catalog does not mutate `random_all`, Challenge competitive pools, or Battle pools;
- Mini App no longer depends on chapter-specific Chapter 2/3 scripts;
- Python imports, JS syntax/unit tests, full pytest, and Docker production/web smoke are CI gates.
