# Runtime + Course Integration Reconciliation

This is the semantic reconciliation record for `agent/runtime-course-integration`.
The branch was created from actual `main` SHA
`e4dea87d7348ee940bc628f7f8d53379e05a5a3a`.

Source authorities were inspected at their exact heads:

- runtime/security PR #27: `bc3de1ebddc41b55a1a3bd29496294c69e7e4f6d`
- course-surface PR #28: `5b2ec63fb60fc6496c0484755e8f1a94d13fc067`

The integration does not merge either branch and never resolves overlap by
`ours`/`theirs`. Non-overlapping verified layers were ported by exact tree/blob
identity; overlapping runtime symbols were reconciled by invariant.

## Status vocabulary

- **EXACTLY_PRESENT** — the useful source invariant is present with equivalent
  semantics and no weaker competing implementation.
- **SUPERSEDED_STRONGER** — the final tree intentionally replaces the source
  shape with a stricter implementation of the same authority.
- **PORT_REQUIRED** — neither source alone was sufficient; the final symbol is a
  manual semantic composition and is covered by integration-specific tests.
- **OBSOLETE** — the old behavior/module is intentionally not retained because
  current production architecture has replaced its authority.
- **NOT_YET_PROVEN** — retained only for non-authority responsiveness debt; it
  is not relied upon for correctness/security/scoring.

## Overlapping-symbol reconciliation matrix

| Final symbol / boundary | #27 contribution | #28 contribution | Final status | Final authority |
| --- | --- | --- | --- | --- |
| `web_api.result_store._persist_once` | acknowledged-write requirement; optional optimistic expected-state predicate; durable receipt confirmation | learning receipt caller/replay semantics | **PORT_REQUIRED** | Unified helper rejects explicit unacknowledged writes, supports scored CAS, and returns the durable receipt for retry classification. |
| `web_api.result_store._sync_weekly_challenge_result` | duplicate-key-safe first insert; monotonic score/time update; acknowledgement + reread proof | retry invokes projection from stored Challenge receipt | **SUPERSEDED_STRONGER** | #27 projection retained; #28 retry path feeds its durable `week_id`. |
| `web_api.result_store._validated_learning_receipt` | none | kind/course/score/total identity; zero points/bonus/achievements; mismatch fail-closed | **EXACTLY_PRESENT** | Learning replay authority. Legacy safe receipts without score/total remain deploy-compatible; new receipts bind score+total. |
| `web_api.result_store._apply_learning_result_once` | acknowledged atomic Mongo update primitive | progress-only Chapter persistence and strict learning receipt identity | **PORT_REQUIRED** | Atomic chapter attempts/correct/total/best only; never mutates scored aggregate economics. |
| `web_api.result_store.apply_regular_result_once` | shared `total_tests` optimistic CAS prevents two distinct scoring results computing Daily economics from one snapshot | route learning pools to progress-only path | **PORT_REQUIRED** | Scored result uses #27 CAS; Chapter2–5 use strict #28 learning path. |
| `web_api.result_store.apply_challenge_result_once` | shared aggregate CAS; stable receipt; single Daily/Challenge economics; monotonic weekly projection; dotted achievement writes | course architecture must never redirect normal chapter pools into Challenge | **SUPERSEDED_STRONGER** | #27 Challenge persistence retained; course boundary cannot select it for normal learning. |
| `web_api.result_store._prune_old_receipts` | recoverability-aware receipt retention | same retention intent | **SUPERSEDED_STRONGER** | Final cleanup additionally checks explicit Mongo acknowledgement without making pruning result authority. |
| `web_api.quiz_start._resolve_normal_course` | hardened Mini App session implementation remains downstream | catalog-bound course authorization, legacy `pool_key` compatibility only | **SUPERSEDED_STRONGER** | `course_key` is normal-learning authority; `pool`, ranking and multiplier-looking client fields fail closed. |
| `web_api.quiz_start.start_quiz` -> `web_api.quiz` durable primitives | owner/open-session lifecycle, strict Mongo insert semantics, answer/finalization/recovery state machine | server-authorized course/pool/count/mode boundary | **PORT_REQUIRED** | Authorization happens before durable session creation; core session/scoring state remains server-only. |
| `questions.pool_policy.POOL_POLICIES` | runtime scored persistence remains compatible with legacy points map | rich `PoolPolicy` registry | **SUPERSEDED_STRONGER** | Chapter2–5 = learning, non-ranked, 0 points; unknown policy fails closed in catalog availability. Narrow chapter-specific set patches are not authority. |
| `course_catalog.course_available` | none | declarative surface + pool/policy/size validation | **EXACTLY_PRESENT** | Sole normal-learning presentation/availability decision. Ch4/5 remain predeclared but hidden until canonical registry supplies >= default count. |
| `course_catalog.resolve_course_pool` | canonical question source remains `questions` | revalidates catalog immediately before start | **EXACTLY_PRESENT** | Availability and start resolve the same canonical registry object; synthetic integration tests mutate that object in place. |
| `telegram_production._start` | PR #27 did not introduce a competing implementation (its blob was task-start main) | catalog-aware deep-link resolution; unknown token stripped before legacy fallback | **SUPERSEDED_STRONGER** | Unknown deep link cannot restore `bot.py` course authority. |
| `telegram_production._menu_command` | no course authority | argument stripping prevents `/menu <legacy-course>` bypass | **SUPERSEDED_STRONGER** | `/menu` is main-menu only; course deep links belong to catalog-aware `/start`. |
| Telegram `/test` + `course:*`/legacy/stale callbacks | durable controller/session authority from runtime layer | catalog resolver; malformed/stale fail gracefully; callback ack before durable launch | **PORT_REQUIRED** | Telegram presentation uses catalog, current Telegram user remains session owner/authorization principal. |
| `legacy_attempt_finalize` + `legacy_learning_result_store` | runtime attempt identity/finalization/receipt state machine | learning-only Chapter result persistence | **PORT_REQUIRED** | Telegram Chapter learning persists the same zero-economics progress policy as Mini App. |
| `tests/test_production_legacy_allowlist.py` | writer reachability/security fence | removes old course/menu handlers and asserts catalog production routing | **SUPERSEDED_STRONGER** | Final test keeps both writer-boundary and course-authority constraints. |
| `miniapp/course_catalog.js` + `miniapp/app.js` | request/session correctness remains server-owned | server catalog rendering, refresh/error/resume behavior, no chapter-specific JS truth | **EXACTLY_PRESENT** | Client never becomes scoring or question-pool authority. |
| legacy `bot.py LEVEL_CONFIG` | transitional historical source only | production no longer reads/patches it for normal learning | **OBSOLETE** | Literal may remain in monolith for standalone history, but is not in production command/callback authority graph. |

## Cross-feature concurrency proof added by this integration

`tests/test_runtime_course_concurrency.py` forces real thread interleavings through
an atomic Mongo model rather than calling operations serially. It proves:

1. two distinct scored Mini App result IDs both persist while only one Daily
   bonus is economically available;
2. duplicate concurrent retry of the same scored result is exactly-once;
3. learning same-attempt concurrent retry is exactly-once;
4. learning replay with a different score or total fails closed;
5. two legitimate independent learning attempts both accumulate;
6. Mini App scored persistence and legacy Telegram scored persistence serialize
   on the same durable aggregate CAS;
7. Chapter learning can run concurrently with a scored result without entering
   scored totals/points;
8. a weekly first-insert race converges monotonically after duplicate-key race;
9. an explicitly unacknowledged result write is never treated as durable.

## Future Chapter proof

`tests/test_runtime_course_catalog_integration.py` adds synthetic Chapter4 and
Chapter5 pools to the canonical `questions.POOL_REGISTRY` object without copying
any authoring banks into this branch. It proves both chapters automatically
surface on Telegram and Mini App, can be resolved at start time, and retain
`learning / ranked=False / points=0` while `COMPETITIVE_POOL`, `BATTLE_POOL`, all
Challenge pools and `random_all` remain byte-for-byte ID-equivalent snapshots.

The same integration test fixes the Chapter3 release invariant explicitly:
normal reviewed Chapter3 contains **165** cards, competitive authority contains
**exactly 12** authorized IDs, and Chapter3 normal policy remains learning-only,
non-ranked and zero-point.

## Current-semantics audit of historical PR #2 / #3 / #4

This audit used the files and handler graph present in the integrated current
runtime, not the descriptions in the old PRs.

| Historical surface | Current production evidence | Final classification |
| --- | --- | --- |
| PR #2 webhook secret-before-parse ingress, PTB bridge readiness, ordered drain/shutdown, request-envelope split | `web_api/__init__.py`, `web_api/telegram_transport.py`, `keep_alive.py`; current allowed updates are only message + callback query because no production inline-query consumer exists | **SUPERSEDED_STRONGER** |
| PR #2 historical `inline_query` webhook allowance | No current inline-query handler in production graph | **OBSOLETE** |
| PR #3 `battle_consistency.py` module shape | Module is absent from current runtime; `battle_integrity.py` owns participant role, opponent claim, answer/result CAS, non-evicting reward receipts and finalization | **OBSOLETE** module / **SUPERSEDED_STRONGER** semantics |
| PR #3 bounded reward receipt list / delete-after-best-effort delivery | Current non-evicting digest receipts + per-recipient durable outbox/lease/ack/release retain evidence until delivery settles | **OBSOLETE** |
| PR #4 `session_boundaries.py` module shape | Module is absent; `session_integrity.py` binds session + current user + attempt identity and CAS state | **OBSOLETE** module / **SUPERSEDED_STRONGER** semantics |
| PR #4 UUID/session-id-as-authorization assumptions | Current user identity is the authorization principal; attempt/session tokens are locators/freshness evidence | **OBSOLETE** |
| remaining synchronous legacy read-only leaderboard/history/admin presentation | Not scoring, reward, finalization, Battle or session authority | **NOT_YET_PROVEN** responsiveness-only debt; correctness does not rely on it |

## Second architecture audit after first green

The first integrated tree reached CI/Security/CodeQL green before this audit.
The second pass then checked duplicated authorities, import boundaries, policy
widening, forgotten legacy command paths and transaction races.

Findings/actions:

- no second runtime course map is registered in production; `/test`, `/start
  <course>`, `/menu`, new callbacks and stale compatibility callbacks cannot
  promote `bot.py LEVEL_CONFIG` back to authority;
- `POOL_REGISTRY` and the private build map are the same canonical dict object;
  synthetic future registration must mutate that registry object rather than
  artificially rebind only its exported alias;
- Mini App accepted otherwise-unused `pool` and `multiplier` fields. They could
  not alter scoring, but accepting policy-looking client authority was weaker
  than the fail-closed API contract. The integration now rejects them alongside
  ranked/scoring/points/score-multiplier overrides;
- Chapter3 competitive snapshots and synthetic Chapter4/5 tests prove no policy
  widening;
- result-store thread-interleaving tests prove the manually reconciled CAS and
  learning paths can coexist without lost updates or duplicate economics;
- import-cycle regression tests from #28 remain in the full suite;
- no additional correctness/security authority remains intentionally deferred.

## Container-level end-to-end gate

The production Docker image is started on an ephemeral Docker network beside a
real Mongo container. `scripts/container_e2e_smoke.py` exercises the built HTTP
service and proves:

- `/live` and database `/ready`;
- webhook cold readiness fails closed until PTB bridge configuration;
- deliberately wrong webhook secret returns 401 before malformed JSON parsing;
- `/api/catalog` + Mini App HTML/assets;
- no catalog ranking/pool/persistence/scoring-multiplier internals;
- authenticated public question compatibility endpoint exposes only
  `id/question/options`, never correct answer or explanation/source data;
- ambiguous client policy-looking start fields return 400;
- normal course quiz start, all ten answer transitions, durable result, and
  persisted `/api/me` aggregate through the built production image.

No production secret or external mutable service is used by this smoke.
