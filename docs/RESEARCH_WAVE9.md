# Research WAVE 9 — PvP battle consistency and recovery

Date: 2026-08-10

## Scope

This wave audits the legacy Telegram PvP battle flow independently from the Mini App hardening baseline (PR #1) and independently from the webhook transport migration (PR #2).

Branch: `agent/bible-bot-battle-consistency`
Base: `agent/bible-bot-production-repair`

## Confirmed legacy defects

### 1. Concurrent opponent join overwrite

Legacy join flow performed:

1. read battle;
2. check `status == waiting` and `opponent_id is None`;
3. call generic `update_battle()`.

`update_battle()` is an unconditional `$set` by `_id`. Two joiners could therefore both pass the read-side checks and the later write could overwrite the earlier opponent.

### 2. Concurrent double finalization / double reward

Both players could independently observe that both `creator_finished` and `opponent_finished` were true and enter result finalization.

Legacy `update_battle_stats()` is non-idempotent `$inc`, including score rewards. Concurrent or repeated finalization could increment `battles_played`, win/loss/draw counters and `total_points` more than once.

### 3. Callback role trusted over Mongo identity

`start_battle_questions()` accepted role suffixes from callback data (`creator` / `opponent`) without verifying the current Telegram user against the battle document.

### 4. Battle cancellation lacked participant authorization

`cancel_battle()` deleted by battle ID without requiring the requester to be the creator or opponent.

### 5. Late last-answer callback race

`processing_answer` was released before the transition into `finish_battle_for_user()`. A delayed duplicate answer callback could re-enter after `current_question` became `len(questions)` and attempt to index beyond the question array.

### 6. Crash after second finish could strand the battle

Even after making finish recording atomic, a process crash after both `*_finished=True` but before rewards/messages could leave a completed battle with no result finalization.

### 7. Partial result delivery was previously lossy

Legacy result delivery attempted to send the result to both users and then deleted the battle even if one Telegram send failed. One participant could therefore permanently miss the result.

## Atomic consistency layer

`battle_consistency.py` isolates race-sensitive Mongo operations from the large legacy handler file.

### Atomic join

`join_battle_atomic()` uses one `find_one_and_update` predicate requiring:

- matching battle ID;
- `status == waiting`;
- `opponent_id == None`;
- creator ID different from the joining user.

Only one concurrent contender can claim the opponent slot.

### Server-authoritative role

`battle_role_for_user()` derives `creator` / `opponent` from the battle document. Handler callback role is accepted only when it matches this Mongo-derived role.

### Atomic finish handoff

`record_battle_finish_atomic()` requires:

- battle ID;
- role-specific participant ID equals the Telegram user;
- role-specific `*_finished` is not already true.

It records score/time/points/finished in one Mongo operation and returns the post-update battle.

For two concurrent finishers, both participant writes may succeed, but only the second returned document can contain both finished flags. This gives a natural single-finalizer handoff without a read/write race.

### Exactly-once battle rewards

Each user document stores a bounded `battle_reward_receipts` list. `apply_battle_reward_once()` atomically combines:

- predicate that the battle receipt is absent;
- battle counter/point `$inc`;
- receipt `$push` with a bounded last-100 slice.

Because receipt and counters live in the same Mongo document update, duplicate finalizers cannot double-award the same user for the same battle.

The helper distinguishes:

- newly applied reward;
- already-applied receipt;
- missing user;
- retryable Mongo ambiguity/error.

A retryable error prevents battle deletion.

## Retryable finalization lease

A finished battle is finalized through a 30-second lease.

`claim_battle_finalization()` can claim a battle only when both players are finished and:

- no result state exists;
- result state is pending;
- or a previous `finalizing` lease expired / lacks a claim timestamp.

The claim records `result_state=finalizing` and `result_claimed_at_dt`.

If the process dies after the second finisher but before completion, `cleanup_old_battles_job()` finds finished battles whose lease is available and retries `show_battle_results()`.

The existing stale battle cleanup only deletes old `status=waiting` battles, so it does not race with finished `in_progress` recovery documents.

## Retryable result delivery

Result delivery is tracked independently from reward receipts.

After a successful Telegram `send_message`, the battle document receives an idempotent per-role delivery receipt:

- `creator_result_delivered`;
- `opponent_result_delivered`.

On recovery:

- an already-delivered participant is skipped;
- a failed participant is retried;
- the battle is deleted only when both delivery receipts are durably present.

If final deletion itself fails, the lease recovery can retry deletion later without re-sending already receipted results.

There remains an unavoidable narrow at-least-once messaging window: Telegram may accept a message and the process/database may fail before the delivery receipt is persisted. A later retry can duplicate that one result message. Avoiding this would require an external transactional message-delivery system, which is not justified for this small bot.

## Participant-only cancellation

`cancel_battle_for_participant()` deletes only when the requester matches `creator_id` or `opponent_id`.

## Late callback serialization

The battle answer handler now keeps `processing_answer` through next-question / finish transition. It also guards `current_question >= len(questions)` before indexing and retries finish instead of touching an invalid question index.

Successful finish removes that user's active battle session from in-memory `user_data`; the `finally` block only releases a still-current session object.

## Async maintenance improvement

Battle recovery and stale battle cleanup are called through `asyncio.to_thread` from the JobQueue callback, so these maintenance Mongo calls no longer synchronously block the PTB event loop.

## Test coverage

The wave adds tests for:

- two concurrent opponent joins;
- creator self-join rejection;
- server-authoritative role mapping;
- participant-only cancellation;
- two concurrent finishers with exactly one second-finisher handoff;
- wrong-role and duplicate finish rejection;
- concurrent exactly-once rewards;
- partial reward retry convergence;
- idempotent draw rewards;
- bounded reward receipts;
- missing user / retryable DB classification;
- finalization lease exclusivity and recovery after expiry;
- incomplete battle exclusion from finalization;
- idempotent participant delivery receipts;
- delete refusal until both result delivery receipts exist.

The final clean branch runs the inherited CI suite, Docker build/runtime smoke, PyPA security audit and a dedicated stacked-PR CodeQL gate.

## Handler integration safety

`bot.py` is a large legacy file. Its PvP edits were applied only through temporary one-shot exact-byte workflows with these guards:

- every source block had to occur exactly once;
- replacement blocks had to be absent before the patch;
- reverse replacement had to reproduce the original bytes exactly;
- only `bot.py` could appear in the patch scope.

Each temporary write workflow was deleted immediately after its guarded patch. No temporary write workflow remains in the final PR diff.

## Accepted residual risks / follow-ups

1. Telegram result delivery is at-least-once, not exactly-once, in the narrow send-success / receipt-write-failure window.
2. A participant who permanently blocks the bot can keep a completed battle pending for delivery. A future operational policy can classify permanent Telegram `Forbidden` errors or expire very old finalizations rather than retry forever.
3. Other legacy async handlers still execute synchronous Mongo calls on the PTB event loop. Those are an availability/performance concern and are intentionally separated from this PvP correctness PR.
4. Session callback ownership (`resume_session_*`, `restart_session_*`, `cancel_session_*`) is a separate authorization finding and will be addressed in the next isolated wave.
