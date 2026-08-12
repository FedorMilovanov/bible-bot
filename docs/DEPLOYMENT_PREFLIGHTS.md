# Deployment preflights

Run the Mongo checks from an **authorized environment that already has the production `MONGO_URL`** before changing draft/merge/deploy state. These commands are intentionally read-only: they inspect production state and return a decision signal, but they do not choose winners, delete duplicate rows, or create/drop/replace indexes.

## Exit codes

All preflight commands use the same operational convention:

- `0` — the inspected contract is safe for the check;
- `1` — the external system was reachable, but the inspected data/index/webhook contract is unsafe and requires operator review;
- `2` — the preflight could not establish the contract because configuration or the external system was unavailable.

Do not treat exit `2` as success. Do not convert exit `1` into an automatic repair step.

## Pre-deploy Mongo checks

### 1. Legacy Telegram duplicate sessions

```bash
python scripts/check_active_session_duplicates.py
```

Checks for more than one `status="in_progress"` quiz session per user. The runtime `uniq_active_quiz_user` index protects exactly this state.

If duplicates exist, stop. Resolve the contradictory rows through an explicitly reviewed migration before attempting the unique-index rollout.

### 2. Mini App duplicate open sessions

```bash
python scripts/check_miniapp_session_duplicates.py
```

Checks for more than one open Mini App session per user across:

- `in_progress`
- `finalizing`
- `score_error`

These statuses exactly match the runtime Mini App open-session uniqueness contract.

If duplicates exist, stop. The preflight deliberately does not mark an arbitrary session abandoned or select a winner.

### 3. Exact session unique-index contracts

```bash
python scripts/check_session_unique_indexes.py
```

Checks the current MongoDB index metadata without modifying it:

- `quiz_sessions.uniq_active_quiz_user`
  - key: `user_id`
  - `unique=true`
  - partial filter: `status="in_progress"`
- `miniapp_sessions.uniq_miniapp_active_user`
  - key: `user_id`
  - `unique=true`
  - partial filter: `status in [in_progress, finalizing, score_error]`

A missing index and an incompatible existing index both return an unsafe result. They are not equivalent operationally:

- a **missing** index may be created by the strict runtime installer once duplicate checks are clean;
- an **incompatible existing Mini App unique index is intentionally preserved by runtime startup** and requires an operator-reviewed migration before deploy. Runtime does not drop that guard automatically during a rolling deploy.

After any authorized index migration, rerun this preflight and require exit `0` before proceeding.

### 4. Durable-evidence retention / TTL contracts

```bash
python scripts/check_retention_indexes.py
```

Checks the terminal-only retention contracts for:

- legacy Telegram quiz sessions;
- Mini App sessions;
- finalized-and-delivered PvP battles;
- admin-delivered reports;
- completed broadcast parent records;
- broadcast recipient delivery rows, retained only after the entire immutable fanout is terminal.

Broadcast TTLs must use `retention_at_dt`, not `created_at_dt`. Runtime sets that retention timestamp only after every recipient row has reached a terminal delivered/permanent-failure state, so a partially delivered broadcast cannot lose old delivery receipts and recreate them as unsent work.

On the **first deploy that introduces the broadcast collections**, the two exact broadcast TTL indexes may legitimately not exist yet. That one condition returns exit `0` with a `bootstrap_pending` entry naming the missing index and `action=runtime_create_before_http`. This is safe because production startup creates those non-destructive indexes before HTTP/Telegram become reachable. It is not permission to ignore any existing TTL index: an age-based legacy TTL, an unrecognized TTL, or an incompatible existing target index remains exit `1`.

After that first deploy, rerun this retention preflight. Both broadcast TTLs must then be present under their exact names/options and the successful result must no longer contain `bootstrap_pending`.

The preflight also reports unsafe historical generic TTL indexes whose age-only deletion could destroy unfinished or undelivered recovery evidence.

The command only reads `index_information()`. It does not run the runtime retention migration or create broadcast indexes.

### 5. Result-receipt BSON growth and Mongo topology

```bash
python scripts/check_result_storage_growth.py
```

Measures the largest leaderboard user documents with Mongo `$bsonSize`, counts embedded non-evicting receipt maps, reports malformed receipt maps, and classifies the Mongo topology as standalone / replica set / sharded.

This is a capacity/readiness check, not a cleanup command. Do not delete old idempotency receipts merely to make the report smaller: those receipts are what prevent replayed results from minting points twice.

## Code/deployment gate

Before deployment, require:

1. both duplicate preflights exit `0`;
2. the session unique-index preflight exits `0` after any explicitly reviewed index migration;
3. the retention preflight exits `0`; before the first broadcast-aware deploy only the two explicitly reported broadcast `bootstrap_pending` entries are acceptable;
4. the BSON/storage-growth preflight has no warning or malformed-map result requiring investigation;
5. the current PR head passes CI, Security Audit, and CodeQL after any migration-related code change;
6. `render.yaml` still declares `numInstances: 1`, `TELEGRAM_TRANSPORT=webhook`, `TELEGRAM_WEBHOOK_MAX_CONNECTIONS=1`, and `autoDeploy: false`.

Production startup re-verifies safety-critical session indexes and durable broadcast indexes before Telegram transport begins. That runtime fail-fast boundary is a backstop, not a substitute for running these preflights before rollout.

## Post-deploy Telegram webhook check

After an authorized webhook deployment, run from an environment that already has `BOT_TOKEN` and either `RENDER_EXTERNAL_URL` or `TELEGRAM_WEBHOOK_BASE_URL`:

```bash
python scripts/check_telegram_webhook.py
```

This command calls only Telegram `getWebhookInfo`. It never calls `setWebhook`/`deleteWebhook`, never changes MongoDB and never prints `BOT_TOKEN`.

Exit `0` requires all of these exact contracts:

- deployed URL equals the expected HTTPS origin plus `/telegram/webhook`;
- `max_connections=1`;
- `allowed_updates` is exactly `message` + `callback_query`;
- no recent Telegram delivery error is present.

A non-zero `pending_update_count` is reported as a warning rather than an automatic failure because a short transient queue is valid during cold start. Re-run the check after traffic; continuously growing pending updates require investigation.

An old retained Telegram delivery error is also a warning. By default an error from the last 300 seconds is unsafe; adjust only for a reviewed incident with `TELEGRAM_WEBHOOK_ERROR_MAX_AGE_SECONDS`.

## Webhook rollout verification

Render production uses the existing Waitress server for `POST /telegram/webhook`; no PTB webhook server or self-ping keepalive is used. Polling remains the explicit rollback transport.

After the first authorized deploy:

1. Confirm `GET /live` returns success.
2. Confirm `GET /ready` reports Mongo ready.
3. Confirm `GET /telegram/ready` returns `200` with `transport=webhook` after PTB startup.
4. Rerun `python scripts/check_retention_indexes.py`; require exit `0` **without** broadcast `bootstrap_pending`.
5. Run `python scripts/check_telegram_webhook.py` and require exit `0` (warnings must be understood, not ignored).
6. Exercise `/start`, normal quiz answers, timed/speed answer timeout, Challenge 20, retry-errors, report submission, PvP create/share/deep-link/join/finish, `/status`, restart and cancel.
7. Exercise one small administrator `/broadcast` and verify it is durably accepted before delivery; after a controlled restart, verify pending recipient rows continue instead of restarting the entire recipient list.
8. Verify report/PvP/broadcast delivery recovery after a controlled application restart.
9. Let the Free Render service become idle long enough to spin down, then send a Telegram update. The first webhook request may encounter cold-start unavailability; the service must wake and Telegram must retry until it receives a 2xx response. Verify the update is eventually processed through the hardened production handler graph.
10. Run `python scripts/check_telegram_webhook.py` again and make sure pending updates are not continuously growing and there is no current delivery error.

A Telegram send cannot carry a server-side idempotency key. If the process dies after one recipient send succeeds but before its Mongo acknowledgement, that one recipient may receive the message again after lease recovery. Durable fanout/receipts intentionally confine this at-least-once uncertainty to the in-flight recipient; a restart cannot begin the whole broadcast from recipient zero.

Do not treat a successful `/live` alone as proof that Telegram ingress or Mongo authority works.

## Polling rollback

If webhook delivery itself must be isolated during an incident, change only the transport configuration to:

```text
TELEGRAM_TRANSPORT=polling
```

and redeploy the **same** application code. PTB polling is retained specifically as rollback and uses the same production handlers/state authority. Do not re-enable legacy `bot.py` as the launcher.

After rollback, verify `/start`, one quiz answer, `/status`, report flow, one PvP action and durable `/broadcast` acceptance. When returning to webhook mode, repeat the webhook preflight above.

## No automatic repair

None of these checks or rollout steps is permission to:

- choose an arbitrary duplicate session winner;
- delete unfinished/finalizing/score-error evidence;
- drop an incompatible unique guard during a rolling deploy;
- delete unfinished broadcast parent/delivery rows to reduce storage;
- clear non-evicting scoring receipts merely to shrink BSON;
- create a self-ping/keepalive loop to defeat hosting sleep behavior;
- mutate Telegram webhook state from a diagnostic preflight;
- paste production secrets into CI logs, PR comments or chat.

Any production data/index migration requires an explicit reviewed plan, followed by all five Mongo preflights again.
