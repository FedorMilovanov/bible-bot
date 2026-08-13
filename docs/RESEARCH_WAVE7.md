# Research WAVE 7 — async lifecycle, calendar correctness and observability

Date: 2026-08-09

## Scope

This wave closes the remaining P1 correctness findings carried out of WAVE 6 and adds a small protection layer for public observability endpoints. The legacy Telegram handler graph remains unchanged; the changes are isolated to the Mini App, its API adapters, the weekly leaderboard date helper and CI coverage.

## Implemented

### 1. Challenge recovery preserves the original ISO week

A Challenge result can be persisted to the user's exactly-once receipt before the weekly leaderboard write succeeds. If that secondary write fails near an ISO-week boundary, a later retry must not move the result into a new week.

The Mini App result receipt now stores `week_id` at first application. Recovery uses that stored value. For receipts created before this field existed, the original ISO week is derived from `applied_at`, so the migration is backward-compatible.

The Mini App weekly helper uses `date().isocalendar()` and therefore uses the ISO year as well as the ISO week number. Regression coverage includes:

- 2021-01-01 -> `2020-W53`;
- 2021-01-04 -> `2021-W01`;
- retry after a simulated week boundary;
- recovery from a legacy receipt without `week_id`.

### 2. Legacy weekly leaderboard now uses the ISO year

`database.get_current_week_id()` previously combined an ISO week number with the ordinary calendar year. That is incorrect around New Year, where early-January days can belong to the final ISO week of the previous ISO year.

The legacy helper now uses both `iso_year` and `iso_week`. The full-file GitHub Contents API update was verified with a commit compare: `database.py` changed only in the intended week-id lines.

### 3. Mini App quiz-session insert failures have correct HTTP semantics

`start_quiz()` no longer maps every `insert_one` failure to a `409`.

- `DuplicateKeyError` -> `409 another active quiz already exists`;
- other `PyMongoError` -> retryable `503 database temporarily unavailable`;
- unexpected application exception -> `500 could not create quiz session`.

All three cases have regression tests.

### 4. Removed the redundant second Challenge weekly write

The Mini App result store is now the single owner of Challenge weekly synchronization. `_finalize_quiz()` no longer calls the legacy `update_weekly_leaderboard()` after `apply_challenge_result_once()`.

This is important for recovery: the result-store write is receipt-aware and preserves the original week, while the old second write used the current week at retry time.

### 5. Mini App asynchronous quiz flow is generation-guarded

The browser previously scheduled an unconditional delayed transition after every answer. If the user exited the quiz during the feedback delay, the pending callback could still load the next question or show the result in the background. Late `start/current/answer` responses could similarly update a newer screen.

A small standalone `miniapp/flow_guard.js` now owns an epoch/generation token and at most one delayed quiz transition. The Mini App:

- starts a new generation for each quiz launch;
- invalidates the generation on quiz exit/home;
- ignores late `start`, `current` and `answer` responses from stale generations;
- cancels a delayed transition when a flow is invalidated or replaced;
- verifies both generation and `session_id` before applying async results.

The guard is independently testable without a browser DOM. CI runs Node's built-in test runner and currently covers:

- a new flow invalidates the previous generation;
- invalidation cancels delayed transition;
- only the most recently scheduled transition can fire.

The production-container smoke test additionally fetches `/flow_guard.js` and confirms that the HTML loads it before `app.js`.

### 6. Closing confirmation lifecycle has one owner

The old unconditional `enableClosingConfirmation()` call in `app.js` was removed. `miniapp/lifecycle.js` remains the sole owner of Telegram close-confirmation state and enables it only while the quiz screen is active.

### 7. Public observability reads are briefly cached

`/live` remains completely database-independent.

`/ready`, `/health` and `/stats` are public operational surfaces. Repeated requests previously caused a MongoDB ping on every readiness/health request and a user count on every stats request. A tiny thread-safe `TTLValueCache` now coalesces only these non-critical observability reads:

- database readiness: 2 seconds;
- total user count: 15 seconds.

Quiz state, profile state, leaderboards, scoring and authenticated user data are not cached by this layer.

The cache uses `time.monotonic()`, a lock around refresh, explicit `clear()`, and loader identity so tests/monkeypatches cannot accidentally reuse a value from a replaced data source.

## Validation

Validated code head before this documentation commit: `d61a02727b409871e23f23f7c62c4c161bc67b8f`.

Main CI: **green**

- actionlint: passed;
- dependency install: passed;
- `pip check`: passed;
- tracked-tree secret guard: passed;
- Ruff: passed;
- Python compile: passed;
- pytest: **82 passed**;
- JavaScript syntax checks: passed;
- Node Mini App unit tests: **3 passed, 0 failed**;
- production Docker build: passed;
- built-container `/live`, Mini App root and `/flow_guard.js` smoke: passed.

Independent security gates: **green**

- PyPA `pip-audit`: passed;
- CodeQL Python: passed;
- CodeQL JavaScript/TypeScript: passed.

## Primary references

- Python `date.isocalendar()` / ISO year-week semantics: https://docs.python.org/3/library/datetime.html#datetime.date.isocalendar
- MDN `setTimeout()`: https://developer.mozilla.org/en-US/docs/Web/API/Window/setTimeout
- Node.js test runner: https://nodejs.org/api/test.html
- PyMongo errors: https://pymongo.readthedocs.io/en/stable/api/pymongo/errors.html
- Telegram Mini Apps: https://core.telegram.org/bots/webapps
- Telegram Bot API webhooks: https://core.telegram.org/bots/api#setwebhook
- python-telegram-bot custom webhook example: https://docs.python-telegram-bot.org/en/v20.7/examples.customwebhookbot.html
- Render free web-service behavior: https://render.com/docs/free

## Remaining architecture waves

No open P0/P1 scoring or Mini App race finding remains from this audit pass.

The next high-value work should be isolated from this already-green hardening PR:

1. **Polling -> webhook for Free Render.** Use the existing Flask/Waitress HTTP service as the public webhook ingress and feed validated Telegram updates into the PTB `Application.update_queue`; do not start a second public web server. Keep an explicit polling rollback mode because Telegram webhooks and `getUpdates` are mutually exclusive.
2. **python-telegram-bot 20.7 -> current major.** Treat as a separate compatibility migration for the large stateful `ConversationHandler` surface.
3. **Horizontal scaling.** Current per-user locks and rate limiting are intentionally process-local. Move them to a shared/distributed mechanism before running multiple app replicas.
4. **Retention policy.** Current quiz-session collections are TTL-backed recent history. Any move to long-term history storage should be an explicit product/privacy decision, not an incidental technical change.
