# Research Wave 5 — crash-safe scoring and recovery

Date: 2026-08-09

This wave closes a failure mode that ordinary HTTP idempotency does not cover: the process can die after the user aggregate was incremented but before the Mini App session was marked `finished`.

## Applied design

1. **Mini App result receipt lives in the same user-document update as points/attempts.**
   - `miniapp_result_receipts.<session_id>` is written atomically with the aggregate `$inc/$set/$max` update.
   - A retry with the same session id reads the existing receipt instead of applying the aggregate again.
2. **Normal Mini App result persistence is self-contained.** Attempts, total points, daily activity streak, daily bonus, perfect count and max streak are applied in the same user-document update as the receipt.
3. **Challenge result persistence is self-contained.** Challenge attempts, points, daily Challenge bonus, streak and achievements are applied with the receipt. Weekly leaderboard update remains separately retryable because it already stores only a user's best score/time.
4. **`finalizing` and `score_error` are recoverable.** Replaying the last answer can resume finalization rather than leaving a permanently stuck result.
5. **Receipt retention is bounded but recovery-safe.** Receipts older than 24 hours are pruning candidates only when their source Mini App session is absent or terminal (`finished`/`abandoned`). Receipts for `in_progress`, `finalizing` and `score_error` sessions are retained regardless of age.
6. **Crash scenario is regression-tested.** Tests simulate an aggregate write succeeding, the process dying before session completion, then `_finalize_quiz` being retried. `total_tests`, attempts and points remain single-increment.

## Why not a one-bit idempotency flag

A separate `leaderboard_recorded=true` flag is insufficient when it is written in a different Mongo operation than the actual aggregate. A crash can occur between those writes. The receipt must be part of the same atomic document update as the money-like counters it protects.

## Current validation checkpoint

On the first fully integrated receipt head before the next lifecycle race wave:

- actionlint: passed
- dependency install + `pip check`: passed
- tracked-tree secret guard: passed
- Ruff maintained-layer lint: passed
- Python compile: passed
- pytest: **40 passed**
- Mini App JavaScript syntax: passed
- production Docker build: passed
- built-container `/live` + Mini App runtime smoke: passed
- PyPA `pip-audit`: passed
- CodeQL Python: passed
- CodeQL JavaScript/TypeScript: passed

## Primary references

1. https://www.mongodb.com/docs/manual/core/write-operations-atomicity/
2. https://www.mongodb.com/docs/manual/core/transactions/
3. https://www.mongodb.com/docs/manual/reference/operator/update/set/
4. https://www.mongodb.com/docs/manual/reference/operator/update/inc/
5. https://www.mongodb.com/docs/manual/reference/operator/update/max/
6. https://www.mongodb.com/docs/manual/reference/operator/query/exists/
7. https://www.mongodb.com/docs/languages/python/pymongo-driver/current/crud/update/
8. https://www.mongodb.com/docs/languages/python/pymongo-driver/current/crud/transactions/

## Next correctness edge

Starting a new quiz while the previous request is simultaneously committing its last answer is a distinct race from result idempotency. The old implementation bulk-abandoned every `in_progress` session. The next wave should replace that with a compare-and-set lifecycle loop: an unfinished session may be abandoned, but if its index advanced concurrently to completion, its result must be recovered/finalized before a new session is created.
