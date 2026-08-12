# Deployment preflights

Run the Mongo checks from an **authorized environment that already has the production `MONGO_URL`** before changing draft/merge/deploy state. These commands are intentionally read-only: they inspect production state and return a decision signal, but they do not choose winners, delete duplicate rows, or create/drop/replace indexes.

## Exit codes

All Mongo preflight commands use the same operational convention:

- `0` — the inspected contract is safe for the check;
- `1` — MongoDB was reachable, but the inspected data/index/storage contract is unsafe and requires operator review;
- `2` — the preflight could not establish the contract (for example, missing `MONGO_URL` or MongoDB unavailable).

Do not treat exit `2` as success. Do not convert exit `1` into an automatic repair step.

## Recommended order

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

Checks the terminal-only TTL contracts for:

- legacy Telegram quiz sessions;
- Mini App sessions;
- finalized-and-delivered PvP battles;
- admin-delivered reports.

It also reports unsafe historical generic TTL indexes whose age-only deletion could destroy unfinished or undelivered recovery evidence.

The command only reads `index_information()`. It does not run the runtime retention migration.

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
3. the retention preflight exits `0`;
4. the BSON/storage-growth preflight has no warning or malformed-map result requiring investigation;
5. the current PR head passes CI, Security Audit, and CodeQL after any migration-related code change;
6. `render.yaml` still declares `numInstances: 1`, `TELEGRAM_TRANSPORT=webhook`, `TELEGRAM_WEBHOOK_MAX_CONNECTIONS=1`, and `autoDeploy: false`.

Production startup re-verifies safety-critical session indexes before Telegram transport begins. That runtime fail-fast boundary is a backstop, not a substitute for running these preflights before rollout.

## Webhook rollout verification

Render production uses the existing Waitress server for `POST /telegram/webhook`; no PTB webhook server or self-ping keepalive is used. Polling remains the explicit rollback transport.

After the first authorized deploy:

1. Confirm `GET /live` returns success.
2. Confirm `GET /ready` reports Mongo ready.
3. Inspect Telegram `getWebhookInfo` from an authorized environment holding `BOT_TOKEN` and verify:
   - URL ends with `/telegram/webhook` on the expected Render HTTPS origin;
   - `max_connections` is `1`;
   - `allowed_updates` contains only `message` and `callback_query`;
   - `pending_update_count` is not growing continuously;
   - `last_error_message` is absent or not current.
4. Exercise `/start`, normal quiz answers, timed/speed answer timeout, Challenge 20, retry-errors, report submission, PvP create/share/deep-link/join/finish, `/status`, restart and cancel.
5. Verify report/PvP delivery recovery after a controlled application restart.
6. Let the Free Render service become idle long enough to spin down, then send a Telegram update. The first webhook request may encounter cold-start unavailability; the service must wake and Telegram must retry until it receives a 2xx response. Verify the update is eventually processed exactly through the hardened production handler graph.
7. Re-check `getWebhookInfo` after the cold-start test for pending updates or a current delivery error.

Do not treat a successful `/live` alone as proof that Telegram ingress or Mongo authority works.

## Polling rollback

If webhook delivery itself must be isolated during an incident, change only the transport configuration to:

```text
TELEGRAM_TRANSPORT=polling
```

and redeploy the **same** application code. PTB polling is retained specifically as rollback and uses the same production handlers/state authority. Do not re-enable legacy `bot.py` as the launcher.

After rollback, verify `/start`, one quiz answer, `/status`, report flow and one PvP action. When returning to webhook mode, repeat `getWebhookInfo` verification above.

## No automatic repair

None of these checks or rollout steps is permission to:

- choose an arbitrary duplicate session winner;
- delete unfinished/finalizing/score-error evidence;
- drop an incompatible unique guard during a rolling deploy;
- clear non-evicting scoring receipts merely to shrink BSON;
- create a self-ping/keepalive loop to defeat hosting sleep behavior;
- paste production secrets into CI logs, PR comments or chat.

Any production data/index migration requires an explicit reviewed plan, followed by all five Mongo preflights again.
