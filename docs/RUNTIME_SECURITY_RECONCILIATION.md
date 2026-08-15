# Runtime / security reconciliation

This document is the human-readable release record for branch
`agent/runtime-security-reconciliation`, created from exact main SHA
`e4dea87d7348ee940bc628f7f8d53379e05a5a3a`.

The machine-readable, per-old-change A–F reconciliation is
`docs/runtime_security_reconciliation.json`.

The branch does **not** merge Draft PR #2, #3, or #4. Old code is treated as
historical evidence only; current runtime semantics are the authority.

## Reconciliation outcome

Subject to exact-head release gates, the intended disposition is:

- PR #2: **FULLY SUPERSEDED**. Current webhook transport implements the useful
  contracts in a stronger lifecycle model. The old `inline_query` allowance is
  deliberately obsolete because current production has no inline-query
  consumer. The only surviving base drift was the Waitress request-envelope
  fallback, fixed on this branch.
- PR #3: **FULLY SUPERSEDED BY STRONGER IMPLEMENTATION**. Current Battle state
  uses Mongo CAS, non-evicting reward receipts, durable finalization and a
  per-recipient result outbox. The old bounded reward-receipt cap must not be
  restored.
- PR #4: **FULLY SUPERSEDED BY STRONGER IMPLEMENTATION**. Session action tokens
  are locators/freshness evidence; current Telegram user identity is the
  authorization principal. Restart and cancel are durable owner-scoped CAS
  operations and self-review rejects foreign targets.

## Confirmed current-main defects remediated here

1. Waitress fallback request envelope was 64 KiB while Flask/server semantics
   and Render configuration expected a 1 MiB server envelope. Mini App JSON
   remains separately limited to 64 KiB.
2. Session creation paths could treat an explicit unacknowledged Mongo insert as
   durable success. Strict Telegram, Mini App, and legacy create helpers now
   require acknowledged writes and fail closed on exceptions/conflicts.
3. Two different concurrent Mini App scoring result IDs could both observe a
   Daily/Challenge bonus as available and both award it. Scoring is now a
   durable optimistic-CAS operation against the shared user aggregate/receipt
   state; no process-local mutex is used as persistence authority.
4. Weekly-best projection could race during first insert/update. Projection is
   monotonic and duplicate-key-race safe.
5. Correctness-sensitive synchronous Mongo boundaries were still being invoked
   directly from async PTB handlers/jobs. Quiz/Challenge, Battle maintenance and
   outbox, report acceptance/outbox, and broadcast outbox boundaries are now
   thread-offloaded at operation boundaries.

## WAVE-11 disposition on the task-start main

| Finding | Current-main verdict | Reconciliation |
| --- | --- | --- |
| Generated session ID may be returned after failed insert | **PARTIAL** | Exceptions/conflicts were already closed; explicit `acknowledged == false` was not. Fixed and tested. |
| Finish and scoring are separate non-idempotent writes | **FIXED_ALREADY** | Current architecture already uses completion CAS, scoring claim/applied state, deterministic request ID, and durable score receipt. |
| Score can be credited while session remains `in_progress` | **FIXED_ALREADY** | Physical state may remain `result_pending/scoring_state=applied` after a crash, but that state is explicit recovery evidence; the receipt prevents a second credit and retry completes terminalization. |
| Finished history has the same short TTL as transient sessions | **FIXED_ALREADY** | Existing state-aware retention migration replaces unsafe generic TTL with terminal-only retention; recoverable open/pending states are excluded. |
| Synchronous Mongo can block the PTB event loop | **PARTIAL** | Correctness-sensitive and durable-job boundaries are remediated here. Some legacy read-only presentation/admin queries remain responsiveness debt. |
| Distinct concurrent Mini App results can both claim Daily/Challenge bonus | **CONFIRMED** (additional finding) | Fixed with durable aggregate CAS + receipts and concurrent tests. |

## Durable quiz finalization state machine

The current design intentionally avoids a distributed transaction across every
quiz concern. Instead it uses retry-safe durable states:

1. The attempt is owned by `(user_id, session_id, attempt_id)` and completion is
   accepted with a state/attempt CAS.
2. Scoring transitions from unclaimed to claimed and then applied under a
   deterministic scoring request identity.
3. The user aggregate mutation is guarded by a durable score receipt. Daily
   bonus, Challenge accounting, aggregate totals, and achievement effects are
   part of the guarded scoring mutation.
4. A lost response after aggregate credit is resolved by finding the receipt;
   retry must not re-credit.
5. Session terminalization happens after scoring is proven applied. A crash in
   between leaves explicit `result_pending` recovery evidence instead of
   deleting/pretending success.

Therefore "exactly once" means **effectively exactly-once economic effects by
idempotent durable receipts/CAS**, not a claim that Mongo/Telegram delivery can
be made globally transactional.

## Battle consistency / delivery model

- Opponent claim is one Mongo conditional update: waiting state, no opponent,
  and creator != joiner.
- Role is derived from persisted participant identity.
- Answer/result mutation is durable CAS; stale/late callbacks cannot advance a
  superseded question/attempt.
- Reward/stat application is guarded by deterministic SHA-256 receipt keys that
  are not bounded/evicted like the old 100-item array.
- Finalization is retry-safe across partial participant reward application.
- Result delivery is a durable per-recipient outbox with claim token, lease,
  ack/release, and recovery.
- Battle evidence is not destructively cleaned until both terminal deliveries
  are settled.
- Maintenance/outbox Mongo operations run outside the PTB event loop; Mongo CAS
  remains the multi-worker authority.

## Session authorization model

`resume`, `restart`, and `cancel` resolve the durable action using the callback
locator **and the current Telegram user id**. UUID entropy is not authorization.

- Resume requires current ownership and a fresh attempt identity.
- Restart is owner + expected-attempt + lifecycle-state CAS and allocates a new
  attempt identity; duplicate/stale callbacks cannot perform a second restart.
- Cancel is owner-scoped and allowed only for incomplete/recoverable state;
  finalizable/result-pending evidence is preserved.
- Self-review checks the embedded target user against `query.from_user.id`;
  navigation implicitly targets the current user.
- Mongo unavailable/ambiguous/schema-conflict cases fail closed.

## TTL / history

Transient recovery and terminal audit retention are separate concepts.
`legacy_session_retention` migrates unsafe generic session TTL indexes to
terminal-only retention keyed by terminal timestamp. Recoverable
`in_progress`/`result_pending` records are not put under the terminal TTL.
Terminal retention defaults to the product's longer audit/history window
(currently 90 days).

The migration was already wired into the task-start main through the
`session_integrity` import path. A temporary duplicate bootstrap call discovered
during this audit was intentionally reverted after tracing the full import
chain; no redundant migration authority is retained.

## Event-loop classification

### Offloaded on this branch

Latency-sensitive/durable operation boundaries for:

- quiz/Challenge launch, answer CAS, timeout CAS, finalization,
  resume/restart/cancel/status and recovery lookup;
- Battle maintenance/finalization/result outbox;
- report cooldown/acceptance/inaccuracy acceptance and report outbox;
- broadcast durable acceptance/fanout/claim/ack/defer/release/completion.

The offload helpers do not add a process-local lock or retry loop. Durable
concurrency remains in Mongo predicates/receipts.

### Intentionally synchronous

Startup/index/retention hardening runs before the PTB event loop and is intended
to fail closed before readiness.

### Accepted residual responsiveness debt

Some legacy read-only presentation/admin paths in `bot.py` still use synchronous
Mongo reads (for example leaderboard/history/stat presentation). They are not
reward/finalization/session/Battle authority and are not treated as a correctness
mutex. They remain candidates for a separate responsiveness cleanup rather than
expanding this security reconciliation into a wholesale legacy-controller
rewrite.

## Safe production smoke framework / checklist

No real secret may be stored in the repository or printed by smoke tooling.
The automated CI and pytest layers provide the mocked/container portions below;
external checks are only PASS if actually executed against configured
credentials/environment.

| Check | Safe automated evidence / procedure |
| --- | --- |
| `/live` | CI starts the built web container and requires an HTTP success response. |
| `/ready` | Flask tests cover readiness behavior; live probe may use a configured public base URL. |
| `/health` | Not an applicable route in the current runtime; do not invent a PASS. |
| `/telegram/ready` | Transport tests cover PTB readiness state; external probe only when Telegram runtime credentials exist. |
| webhook cold readiness | Mocked Flask/transport tests require 503 before ingress acceptance. |
| wrong webhook secret | Tests send a deliberately wrong value and prove rejection happens before JSON decoding. Never log the configured secret. |
| correct mocked webhook secret | Test-only secret accepts a valid PTB update and bridge submission. |
| invalid `update_id` | Mocked webhook tests reject invalid/bool/non-int identifiers. |
| request-size split | Regression asserts server envelope and Mini App JSON limits remain independent. |
| Mini App API | Flask/API tests cover authenticated session start/answer/result and failure paths. |
| quiz start/answer/finish | Session launch/live-answer/finalizer tests cover CAS, DB failure, replay, and result-pending recovery. |
| Chapter 2 | Existing Chapter 2 route/content/runtime tests remain in full pytest. |
| Chapter 3 | Existing Chapter 3 route/content/runtime tests remain in full pytest. |
| Challenge | Competitive selection + durable session/scoring/bonus tests, including concurrent distinct result IDs. |
| Battle | Barrier/thread join tests, stale/late/duplicate answer/finalization tests, reward receipts, delivery outbox and recovery. |
| persistence recovery | Quiz result-pending, Battle outbox/finalization, report outbox and broadcast outbox recovery tests. |
| production import | CI imports the production controllers inside the built image. |
| container web smoke | CI builds and starts the production image, then probes public web assets/routes. |

### External smoke rule

Before claiming external production smoke, check only whether required variables
are present; print `SET`/`UNSET`, never their values. If Render URL, Mongo, bot
credential and webhook secret are not available together, record **NOT
EXECUTED**, not PASS. Any live execution must be non-destructive: health/readiness
and a deliberately wrong-secret rejection are safe; do not submit scoring,
Battle rewards, broadcasts, or destructive callbacks against production solely
for a smoke test.

## Exact-head release gates

The branch is releasable only when **the same final head SHA** has all of:

1. CI success, including full `pytest -q`;
2. Security Audit success;
3. CodeQL success;
4. Mini App JavaScript syntax/unit tests success;
5. production Docker image build success;
6. production controller imports in the built image;
7. built-container web smoke success.

Historical green runs on earlier heads are evidence during development but do
not satisfy this final gate.

## Prepared old-PR closure text

### PR #2

> Superseded by current production runtime + #27. The old webhook transport
> invariants were re-audited semantically against current main; the surviving
> request-envelope fallback drift is fixed in #27. Current transport provides
> stricter readiness/lifecycle/shutdown/HTTPS/fail-closed setWebhook semantics.
> The old inline_query allowance is intentionally obsolete because production
> has no inline handler. No old-branch merge is required.

### PR #3

> Superseded by current `battle_integrity` + #27. Atomic opponent claim,
> self-join rejection, server-authoritative role, retry-safe terminal outcomes,
> non-evicting reward receipts, finalization recovery, per-recipient durable
> result outbox and safe cleanup are present in stronger form. #27 adds
> event-loop-safe maintenance/outbox execution and real concurrent regressions.
> The old bounded reward-receipt cap is intentionally not ported.

### PR #4

> Superseded by current owner-scoped session-action/lifecycle architecture +
> #27. Session tokens are locators, while `effective_user.id` is authorization;
> restart is owner+attempt CAS one-shot, cancellation is owner/incomplete only,
> self-review rejects foreign targets, and DB ambiguity/outage fails closed. No
> old branch code is required.
