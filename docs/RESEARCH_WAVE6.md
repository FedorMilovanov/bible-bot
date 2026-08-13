# Research WAVE 6 — consistency, auth boundary and privacy

Date: 2026-08-09

## Scope

This wave continued the production audit after the runtime/supply-chain baseline was already green. The goal was to close correctness gaps that only appear under retries, legacy data, malformed authenticated input, or partial MongoDB failure — without rewriting the legacy Telegram bot.

## Implemented

### 1. Legacy duplicate active Mini App sessions are repaired before the unique index

Older data can contain more than one `status=in_progress` Mini App session for one user. A unique partial index cannot be created while those duplicates already exist.

`web_api/db_hardening.py` now:

- groups active sessions by `user_id`;
- deterministically keeps the most recently updated session;
- marks only older documents that are *still* `in_progress` as `abandoned`;
- runs the repair before creating `uniq_miniapp_active_user`.

The status predicate on the repair update avoids overwriting a session that concurrently progressed to `finalizing` or another state.

### 2. Challenge weekly leaderboard sync is recoverable

The user aggregate already used an exactly-once result receipt, but the legacy weekly-leaderboard helper swallowed MongoDB exceptions. That allowed this sequence:

1. points/attempts persisted;
2. weekly leaderboard write failed;
3. quiz session was still marked `finished`;
4. later replay could not repair the missing weekly result.

`web_api/result_store.py` now performs an idempotent Mini App weekly sync that intentionally propagates database failures. If the aggregate receipt already exists, replay retries only the weekly sync and does not increment points/tests again. Equal scores replace the weekly result only when the new time is faster.

### 3. Compatibility question endpoint is authenticated and rate-limited

`/api/questions/<pool>` no longer allows anonymous scraping. It requires Telegram Mini App authentication and exposes only `id`, question text and answer options — never `correct` or `explanation`.

Its rate limit is normalized to one `/api/questions/*` scope. Switching pool paths therefore cannot reset the bucket.

### 4. PyMongo operational failures return JSON 503

Uncaught `PyMongoError` exceptions at the HTTP layer now return a stable JSON `503 database temporarily unavailable`. `DuplicateKeyError` keeps its more specific `409` response.

This matters for short failovers/server-selection failures: the Mini App receives a retryable service error instead of an HTML 500 page.

### 5. Debug impersonation is impossible on Render

Local `X-Debug-User-Id` remains available only when development mode is explicitly enabled. In addition, `RENDER=true` disables this path unconditionally, so accidentally setting `APP_ENV=development` and `ALLOW_DEBUG_AUTH=true` in the Render dashboard cannot enable production impersonation.

### 6. Telegram user schema is validated at the auth boundary

A successfully signed `initData` payload must now contain a positive JSON integer `user.id`. String IDs, booleans, zero and negative values are rejected before route/business logic.

### 7. `/api/me` moved from blacklist to allowlist projection

Previously, almost every user-document field became public unless it started with `_` or was explicitly blacklisted. That made future internal fields public by default.

The profile response now explicitly allows:

- stable profile aggregates;
- battle summary fields;
- streak/achievement summary fields;
- per-level `attempts`, `correct`, `total`, and `best_score` fields for canonical level keys.

Unknown future internal fields and Mini App receipts are private by default.

## Validation

Validated head before this documentation commit: `e39654f47066728821c64f0af1519072614b86bb`.

- actionlint: passed
- dependency installation + `pip check`: passed
- tracked-tree secret guard: passed
- Ruff maintained layer: passed
- Python compile: passed
- pytest: **69 passed**
- Mini App JavaScript syntax: passed
- production Docker build: passed
- built-container `/live` + Mini App runtime smoke: passed
- PyPA `pip-audit`: passed
- CodeQL Python: passed
- CodeQL JavaScript/TypeScript: passed

## Primary references used in this wave

- Telegram Mini Apps — validating data received via the Mini App: https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app
- Telegram Bot API — Web Apps / Mini Apps: https://core.telegram.org/bots/api#webappinfo
- MongoDB — unique indexes: https://www.mongodb.com/docs/manual/core/index-unique/
- MongoDB — partial indexes: https://www.mongodb.com/docs/manual/core/index-partial/
- PyMongo — `AutoReconnect` and connection failures: https://pymongo.readthedocs.io/en/stable/api/pymongo/errors.html
- PyMongo — timeout handling: https://pymongo.readthedocs.io/en/stable/examples/timeouts.html
- Render — default environment variables (`RENDER=true`, commit/branch metadata): https://render.com/docs/environment-variables#default-environment-variables
- Flask — error handling: https://flask.palletsprojects.com/en/stable/errorhandling/
- MDN — `setTimeout()` and `clearTimeout()`: https://developer.mozilla.org/en-US/docs/Web/API/Window/setTimeout

## Remaining isolated work

The following items are deliberately not mixed into this green backend wave:

1. **Mini App asynchronous navigation race** — delayed answer transitions and in-flight `start/current` requests should be invalidated when the user exits the quiz. This requires a focused edit of the large `miniapp/app.js` state machine plus browser-level regression coverage.
2. **`start_quiz()` insert error classification** — its local broad `except Exception` still maps every session insert failure to `409`. The global HTTP layer now correctly distinguishes PyMongo failures, but this local catch prevents that classification for the insert path. Fix this in an isolated `quiz.py` edit rather than rewriting the large module casually.
3. **Polling -> webhook for Free Render** — research supports it as a separate transport migration with explicit rollback. Do not mix it with the already validated production-hardening diff.
4. **PTB 20.7 -> current major** — keep as a dedicated compatibility migration because the bot has a large stateful `ConversationHandler` surface.
