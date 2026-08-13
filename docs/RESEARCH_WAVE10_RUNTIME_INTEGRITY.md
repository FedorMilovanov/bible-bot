# Research WAVE 10 — Runtime integrity after crash-safe persistence foundation

Date: 2026-08-10
Branch: `agent/bible-bot-legacy-correctness`
PR: #5

## Purpose

WAVE 9 and the follow-up scoring work established a separately testable, idempotent result/bonus/recovery foundation. WAVE 10 audits the **live legacy Telegram runtime** that still sits in front of that foundation.

The key distinction is now explicit:

- the persistence primitives are not the remaining bottleneck;
- the live `bot.py` orchestration still contains several pre-foundation assumptions that can destroy or fail to create the durable evidence those primitives need.

This document is intentionally an audit/remediation contract. It does **not** claim these runtime P1s are fixed until the handlers themselves are integrated and the normal gates prove the integration.

## Verified baseline

At the start of this wave, checkpoint `32944e9e7d17e95fd456c7afa3727bcebcc4806a` passed CI, Security Audit and stacked CodeQL.

CI now rejects temporary self-writing patch machinery under:

- `.github/workflows/oneshot-*`
- `.github/scripts/oneshot_*`

This is deliberate. Runtime integration must be a normal reviewable Git change, not a workflow that edits the branch from inside Actions.

## P1-A — Completed result evidence is still destroyed by `/start`

Current `start()` behavior:

1. load `get_active_quiz_session(user.id)`;
2. calculate `current` and question count;
3. if `current >= total`, call legacy `cancel_quiz_session(session_id)`;
4. discard the active session.

This directly bypasses `legacy_session_recovery.py` / `legacy_session_finalize.py`.

A process can therefore die after the final answer is durable but before scoring, then `/start` can cancel the exact Mongo document required to recover that result.

### Required behavior

`/start` must distinguish:

- `current_index < total`: resumable active quiz;
- `current_index == total`: candidate `result_pending`, validate with `completed_result_inputs()` and finalize through `finalize_completed_session()`;
- `current_index > total`: contradictory evidence, never auto-score or auto-cancel as a normal completion.

Cancellation of completed evidence must never be the fallback path.

## P1-B — Quiz launch continues when durable session creation fails

`create_quiz_session()` now correctly returns `None` when Mongo persistence is unavailable/fails. The live launch paths still continue anyway.

Confirmed call sites include:

- `_launch_level_test()`;
- `random_all_start_handler()`;
- `challenge_start()`.

They assign `session_id = create_quiz_session(...)`, then create `user_data` and send the first question without proving `session_id` is durable.

### Failure mode

The user can complete a quiz that has no Mongo session at all. A later process crash has no answer ledger to recover, and an in-memory result id cannot reconstruct the lost session.

### Required behavior

Quiz launch must be fail-closed:

1. cancel/replace the prior session using an owner-scoped operation;
2. create the new Mongo session;
3. **only if a real session id is returned**, create RAM state and send question 1;
4. otherwise show a retryable “session store unavailable” error and do not start the quiz.

No `session_id=None` quiz should enter the normal/challenge runtime.

## P1-C — Answer persistence is neither fail-closed nor idempotent

Current `database.advance_quiz_session()` performs a blind update:

- `$inc: {current_index: 1}`;
- `$inc: {correct_count: ...}`;
- `$push: {answered_questions: ...}`;
- catches any exception and returns `None`.

Live timeout/answer paths call it and can then advance RAM state without proving the Mongo transition succeeded.

There is also no stable per-answer idempotency predicate. If Mongo commits the update but the caller loses the response, retrying the same answer can increment/push again.

### Required protocol: owner/index answer CAS

Replace blind `advance_quiz_session()` ownership of new runtime writes with an owner-scoped helper, conceptually:

`record_owned_quiz_answer(session_id, user_id, expected_index, question_id, answer, is_correct, latency, question_snapshot)`

The atomic predicate must prove at least:

- `_id == session_id`;
- canonical `user_id` owner;
- `status == in_progress`;
- `current_index == expected_index`.

The stored answer record must carry enough identity to make retry classification deterministic, for example:

- `index`;
- `question_id`;
- `user_answer`;
- `is_correct`;
- `latency_seconds` when available;
- `ts`;
- immutable question snapshot or a stable question reference.

Retry semantics:

- first matching transition atomically appends answer + increments counters/index;
- replay of the **same** already-recorded answer returns the stored transition without another increment;
- conflicting replay for the same index is rejected;
- Mongo outage is explicit and handler RAM must not advance.

Only after durable confirmation may the handler mutate:

- `answered_questions` in RAM;
- `correct_answers`;
- `current_question`;
- streak/fastest-answer runtime fields.

This protocol also solves restart parity for `fastest_answer` if answer latency is persisted.

## P1-D — Six-hour TTL can erase unscored recovery evidence

The current `quiz_sessions` TTL index is on `updated_at_dt` with `expireAfterSeconds=21600` (6 hours) for the whole collection.

That conflicts with result recovery. A completed-but-unscored session can disappear if the process/store remains unavailable or the user returns after the TTL horizon.

### Required retention model

Do not solve this by merely picking a larger arbitrary TTL.

Prefer an explicit expiry field, e.g. `expires_at`, with a TTL index on that field:

- active/resumable session: expiry policy appropriate for abandoned quizzes;
- `result_pending`: **no expiry timestamp until result finalization is durable**;
- `finished` / `cancelled`: set `expires_at` to the desired cleanup horizon after the terminal transition.

Mongo TTL indexes ignore documents where the indexed field is absent, which allows pending result evidence to survive without making all historical sessions permanent.

Migration must account for the existing `ttl_updated_at` index before changing retention semantics.

## P1-E — Live result handlers still close session before old multi-write scoring

Both `show_results()` and `show_challenge_results()` still call legacy `finish_quiz_session(session_id)` near the beginning of finalization and only afterward perform the old independent scoring/bonus/achievement/weekly writes.

This is the crash boundary the new result finalizers were built to remove.

### Required behavior

Handlers must become orchestration/UI only:

1. derive live score/time inputs;
2. call `finalize_normal_result()` or `finalize_challenge_result()`;
3. if finalizer raises `LegacyResultFinalizationPending`, keep `result_pending=True`, do not run legacy scoring, and expose safe retry;
4. render points/bonus/new achievements from the finalizer result;
5. remove old duplicate calls (`add_to_leaderboard`, `check_daily_bonus`, legacy Challenge multi-write path, duplicate achievement updates);
6. the finalizer remains owner of session-close-last semantics.

## P1-F — Restart timeout routing mixes normal timed quizzes with Challenge handlers

`_handle_timeout_after_restart()` currently restores session state, records a timeout answer, then uses Challenge send/final-result functions regardless of whether the persisted session is an ordinary timed/speed `mode="level"` quiz.

The old `_restore_session_to_memory()` also does not reconstruct all normal timing/multiplier fields that `legacy_session_recovery.recovery_fields()` now derives safely.

### Required behavior

Use the pure recovery policy as the single runtime reconstruction source.

After a timeout transition:

- Challenge session -> Challenge next-question/result path;
- normal `level` timed/speed session -> normal next-question/result path with recovered `quiz_mode`, `score_multiplier`, and `quiz_time_limit`.

No generic restart helper should hard-code Challenge routing.

## P1-G — Report persistence and delivery are not crash-safe

Database layer currently has three independent problems:

1. `can_submit_report()` fails open when the user collection is unavailable or raises;
2. `insert_report()` can log a failed/missing report insert, still update user cooldown, and still return a generated UUID;
3. cooldown and report persistence are separate writes with no explicit partial-success contract.

Live `report_confirm()` adds another loss window:

- it `pop()`s the in-memory draft before durable persistence is proved;
- admin Telegram delivery happens after that;
- if delivery fails, there is no draft to retry;
- a Mongo-persisted but undelivered report has `admin_delivered=False`, but no durable retry worker/outbox consumes it.

### Required behavior

Persistence API:

- report store unavailable -> fail closed at report start;
- failed `insert_one` -> no report id and no cooldown advance;
- successful report persistence is the primary durable success;
- cooldown update happens only after durable report insertion and any partial cooldown failure is explicit/logged.

Live confirmation:

- do not delete draft before durable acceptance;
- persist report first;
- delivery is a separate state (`pending_delivery` / `delivered` / retry metadata);
- user-facing “sent” must mean either delivered now or durably queued for delivery;
- a retry worker should process persisted `admin_delivered=False` reports with bounded retry/backoff.

This is an outbox problem, not an `except: pass` problem.

## P2 — Battle result notification still has a delivery-only crash window

Battle outcome scoring is now idempotent and the 64-receipt horizon is removed. However, the completed battle snapshot is deleted before both Telegram result messages are durably acknowledged.

A crash after deletion but before delivery can therefore lose notifications while keeping correct score state.

Correct repair is a durable notification/finalization state or outbox, not another RAM boolean.

## P2 — Multiple active quiz sessions are not structurally prevented

The DB has a non-unique `(user_id, status)` index. Launch flows typically call `cancel_active_quiz_session(user_id)` and then create a new session as separate operations.

Two concurrent launch callbacks can race between those operations and create more than one `in_progress` session for one user.

Before introducing a unique partial index, migration must first detect/resolve existing duplicates. A cleaner long-term session transition can atomically supersede the prior active session or use a per-user active-session lease/document.

## Long-horizon note — receipt/map document growth

Non-evicting quiz result receipts, bonus owner maps and PvP outcome markers intentionally remove replay horizons. They therefore grow the user document over time.

Do **not** restore bounded eviction to solve size growth; that reopens replay bugs.

Measure realistic lifetime volume and, before document size becomes material, move immutable receipts to a dedicated collection with unique `(user_id, result_id)` / `(user_id, battle_id)` keys or an equivalent transactional design.

## Implementation order

Recommended next production sequence:

1. durable owner/index answer CAS + handler “RAM only after durable confirmation”;
2. fail-closed quiz session launch;
3. integrate normal + Challenge result finalizers;
4. `/start` completed-session recovery before any cancellation;
5. replace generic 6h session TTL with explicit expiry-state retention;
6. restart timeout routing through the same recovery/runtime policy;
7. report durable acceptance + delivery outbox;
8. battle notification outbox;
9. measure/migrate long-horizon receipt storage.

Each step should be a normal reviewable commit with its own focused regressions and the existing CI/Security/CodeQL gates. Do not reintroduce self-writing one-shot workflows or full-monolith blind replacements.
