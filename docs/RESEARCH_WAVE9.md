# Research WAVE 9 — Legacy Telegram correctness and authorization

Date: 2026-08-10

## Goal

Harden the legacy Telegram-bot interaction layer after the Mini App/API hardening work, without mixing these changes into the Render webhook transport PR.

This wave lives in `agent/bible-bot-legacy-correctness`, stacked directly on the frozen production-hardening branch (`agent/bible-bot-production-repair`). It intentionally does **not** include PR #2 webhook transport.

## Findings fixed

### 1. `/random` was not actually “all themes”

The legacy `/random` pool omitted the canonical `nero` and `geography` pools even though the UI/help described the mode as using all themes. The pool list now includes both historical leaf pools, matching the already-corrected Mini App `random_all` semantics.

### 2. Battle opponent join race

Legacy `join_battle` used a read/check/write sequence:

1. read battle;
2. check `opponent_id is None`;
3. write opponent.

Two users could pass the check concurrently and both receive “battle started”, while the last write won in MongoDB.

`battle_integrity.claim_battle_opponent()` now performs one atomic `find_one_and_update` compare-and-set requiring:

- matching battle id;
- `status=waiting`;
- `opponent_id=None`;
- requester is not the creator.

Only one opponent can claim the slot.

### 3. Battle callback role/participant IDOR

`start_battle_<id>_<role>` previously trusted the role encoded in callback data. The handler now reloads the persisted battle and compares the requesting Telegram user against `creator_id` / `opponent_id`; callback role is accepted only when it matches the persisted participant role.

Battle cancellation is likewise owner-scoped. A battle id is not authorization.

### 4. Legacy quiz-session callback IDOR

The generic DB helpers `get_quiz_session(session_id)` and `cancel_quiz_session(session_id)` are intentionally generic and filter only by id. Legacy callback handlers had used those helpers directly, which meant a callback containing another user’s session id could restore, restart or cancel that session.

`session_integrity.py` adds owner-scoped operations:

- `get_owned_quiz_session(session_id, user_id)`;
- `cancel_owned_quiz_session(session_id, user_id)` — atomic `find_one_and_update` requiring owner + `status=in_progress`.

`resume_session_handler`, `restart_session_handler` and `cancel_session_handler` now use those owner-scoped operations before any restore/mutation.

### 5. Error-review callback IDOR

`review_errors_<uid>_<index>` previously used the user id carried in callback data to select `user_data[target_id]`. It now requires `target_id == query.from_user.id` and reads the current user’s data only.

### 6. Battle stale-button and final-answer robustness

Old battle answer buttons could remain active after the last question. A repeated `ba_*` callback could reach `questions[len(questions)]` and raise `IndexError`.

The handler now checks:

- active battle session exists;
- current question is still within bounds;
- callback option index is within current shuffled options;
- already-processing answers are rejected;
- result-pending retries re-enter finalization safely rather than scoring a question twice.

Finished question keyboards are retired where possible.

### 7. Battle result race and duplicate rewards

Two players can finish almost simultaneously. The old flow allowed both handlers to observe both participants as finished and both process the shared result, potentially duplicating win/loss/draw stats, points and result messages.

The new protocol is:

1. `record_battle_result()` atomically records one participant’s result, scoped by persisted participant id and role. Retries return the already-stored result instead of overwriting it.
2. When both participants are finished, `claim_final_battle()` applies each participant’s outcome through a bounded per-user `battle_result_receipts` list. `$inc` is guarded by `battle_id not in receipts`, so retries cannot duplicate `battles_played`, win/loss/draw or reward points.
3. Only after both user outcome receipts are durable does one handler atomically `find_one_and_delete` the completed battle. Only the handler receiving that deleted snapshot sends the shared result message.

This makes the statistics crash/retry-safe without introducing a separate battle queue or transaction service.

The receipt list is bounded to the most recent 64 battle ids to avoid unbounded user-document growth.

### 8. Cancellation during result synchronization

A local battle session marked `battle_result_pending` cannot be cancelled while the result is being synchronized. This prevents a stale cancel button from deleting the battle in the middle of retryable finalization.

### 9. Inaccuracy-report question drift

The “⚠️ Неточность?” button already encoded the question index (`report_inaccuracy_<index>`), but the handler ignored it and used the current in-memory question. A late click on an old message could therefore report a different question to the admin.

The handler now parses and bounds-checks the clicked question index. Admin delivery is plain text rather than Markdown so repository/user text cannot break Telegram formatting. If admin delivery fails, the user receives a failure message instead of a false success claim.

### 10. Callback parsing / double-answer bugs

Several legacy handlers answered the same callback query at entry and then tried to answer it a second time with an alert on an error path. Those branches can fail with Telegram “already answered / too old” behavior.

Fixed flows include:

- `report_start` — validates report type and cooldown before one callback answer;
- `retry_errors` — safe integer parse, owner check and one answer per branch;
- `review_test_handler` — safe integer parse and negative/out-of-range checks.

`report_start_*` is now limited to keys in `REPORT_TYPE_LABELS`; arbitrary callback suffixes cannot be stored as arbitrary report types.

## Implementation structure

Large legacy `bot.py` was never manually replaced wholesale.

Audited handler changes were applied through temporary guarded exact-region patch jobs. Each one-shot job was allowed to commit only `bot.py` after verifying expected markers and forbidden legacy patterns. Every temporary write workflow/script was removed immediately after the resulting diff was inspected.

Permanent behavior is implemented/guarded by:

- `battle_integrity.py`;
- `session_integrity.py`;
- `tests/test_battle_integrity.py`;
- `tests/test_session_integrity.py`;
- `tests/test_legacy_handler_contracts.py`.

The source-contract tests intentionally prevent future edits from silently returning generic session-id mutations, callback-user authorization, direct non-idempotent battle stats or stale answer indexing.

## Security / CI state at checkpoint

Checkpoint head: `4b4d57aab8e84c56186f56daa32a8e42ad7aa6d1`.

Passed:

- actionlint;
- dependency installation + `pip check`;
- tracked-tree secret guard;
- Ruff on maintained Python layer/tests;
- full Python compile;
- **101 Python tests**;
- Mini App JavaScript syntax checks;
- **3 Node unit tests**;
- production Docker image build;
- built-container runtime smoke (`/live`, Mini App root, `flow_guard.js`);
- independent PyPA `pip-audit` security workflow;
- CodeQL Stacked PR analysis for Python and JavaScript/TypeScript.

## Residual risks / next audit targets

These are intentionally not presented as already fixed:

1. Legacy `create_quiz_session()` currently logs a Mongo insert failure but still returns the generated session id. The bot can then believe a recoverable DB session exists when it never reached MongoDB. This is the next persistence wave.
2. Legacy normal/challenge result aggregation still needs a crash-boundary audit comparable to the Mini App receipt layer. Ordinary stale double taps are already blocked by `processing_answer`/question bounds, but process failure between multiple result writes may still create partial state.
3. Shared battle statistics are receipt-safe, but there remains a narrow **message-delivery-only** window after the completed battle has been atomically deleted and before result messages reach both users. Statistics cannot duplicate; a process crash in that window could lose the display message. A durable notification outbox is not added without evidence that this complexity is warranted.
4. Report persistence/delivery retry semantics need a separate audit: reports are stored before admin delivery, but the user draft is popped before final delivery outcome.

## Relationship to other waves

- PR #1 / WAVE 2–7: Mini App, API, scoring, security, deployment and CI hardening.
- PR #2 / WAVE 8: Render-friendly Telegram webhook transport.
- This WAVE 9: legacy Telegram authorization, battle consistency and callback correctness.

The separation is deliberate so each architectural layer remains independently reviewable and rollbackable.
