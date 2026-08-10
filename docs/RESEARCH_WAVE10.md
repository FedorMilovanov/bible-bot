# Research WAVE 10 — quiz session callback authorization

Date: 2026-08-10

## Scope

This wave isolates authorization/correctness defects in legacy Telegram callback handlers that carry quiz session IDs or user IDs.

Branch: `agent/bible-bot-session-authorization`
Base: `agent/bible-bot-production-repair`

## Confirmed legacy defects

### Resume by session ID without owner check

`resume_session_handler()` fetched `get_quiz_session(session_id)` and restored that document into `user_data[current_telegram_user]` without requiring `db_session.user_id == query.from_user.id`.

### Restart cancelled by session ID before authorization

`restart_session_handler()` fetched a session by ID and immediately called `cancel_quiz_session(session_id)`. Ownership was not checked before cancellation or reuse of the session configuration.

### Cancel by session ID without owner check

`cancel_session_handler()` called `cancel_quiz_session(session_id)` with no participant/owner predicate.

### Error-review target trusted callback data

`review_errors_handler()` parsed `review_errors_{uid}_{idx}` and directly read `user_data[target_id]`. It did not require `target_id == query.from_user.id`.

Session UUID entropy is not an authorization control. Server-side ownership must be part of the state predicate.

## Mongo-authoritative primitives

`session_boundaries.py` adds:

### `get_owned_quiz_session`

Fetches only by:

- session `_id`;
- normalized Telegram `user_id`;
- `status == in_progress` by default.

Mongo errors fail closed.

### `claim_owned_quiz_session_restart`

Uses one `find_one_and_update` requiring:

- session `_id`;
- owner `user_id`;
- `status == in_progress`.

The operation changes the old session to `cancelled` and returns the previous document exactly once. Two delayed/concurrent restart callbacks cannot both claim the same old session and create two restarts from it.

No process-local lock is used; Mongo document atomicity is the source of truth and a slow restart for one user does not serialize unrelated users.

### `cancel_owned_quiz_session`

Cancels only an in-progress session whose owner matches the requesting Telegram user.

## Handler changes

`resume_session_handler` now uses the owner-bound lookup.

`restart_session_handler` now uses the atomic owner-bound restart claim instead of get-by-ID followed by unconditional cancellation.

`cancel_session_handler` now:

- requires an owner-bound cancellation;
- clears in-memory `user_data` only when the active memory session has the same session ID.

`review_errors_handler` now:

- validates callback shape;
- catches malformed integer/index values;
- rejects `target_id != current user_id` before accessing `user_data[target_id]`.

## Regression tests

Tests cover:

- owner-bound lookup and status enforcement;
- explicit terminal-session lookup only when requested;
- two concurrent restart claims with exactly one winner;
- foreign-user restart rejection without mutation;
- owner-only cancellation and one-shot cancellation;
- fail-closed Mongo outages;
- static handler contracts proving legacy handlers use the owner-bound helpers;
- static review-errors ownership guard.

## Legacy-file integration safety

`bot.py` was changed by a temporary one-shot exact-byte patch workflow. The workflow required unique source blocks, reverse byte-for-byte reconstruction and `bot.py`-only diff scope. It was deleted immediately afterwards and is absent from the final PR diff.

## Gates

The clean WAVE-10 code head passed:

- full inherited CI;
- Docker build and built-container runtime smoke;
- Security Audit / PyPA dependency audit;
- stacked PR CodeQL for Python and JavaScript/TypeScript.

## Out-of-scope findings queued for WAVE 11

The session authorization audit exposed separate persistence/scoring defects that are intentionally not mixed into this PR:

1. `create_quiz_session()` logs an insert failure but still returns the generated session ID, so a test can continue memory-only while the caller believes it is persisted.
2. `finish_quiz_session()` hides update failure, while leaderboard/challenge scoring proceeds afterwards; this can create score/session divergence and retry double-credit paths.
3. leaderboard and Challenge stats use non-idempotent `$inc` without a session receipt.
4. the 6-hour TTL on `quiz_sessions.updated_at_dt` applies to finished sessions too, while `get_user_history()` expects finished sessions as history.
5. several legacy async paths still execute synchronous Mongo calls on the PTB event loop.
