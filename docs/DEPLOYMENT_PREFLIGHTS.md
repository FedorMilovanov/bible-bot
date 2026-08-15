# Deployment preflights and release contract

Production deployment is fail-closed. A green code check is not itself proof of a healthy live service, and an HTTP listener is not itself proof that Telegram or MongoDB is usable.

## Exit codes

All standalone preflight commands use the same convention:

- `0` — the inspected contract is safe;
- `1` — the external system was reachable, but the inspected contract is unsafe and requires operator review;
- `2` — the contract could not be established because configuration or the external system was unavailable.

Never treat exit `1` or `2` as success.

## Production release loop

The Render Blueprint is the deployment authority for this service.

`render.yaml` must keep all of these invariants:

- `numInstances: 1`;
- `TELEGRAM_TRANSPORT=webhook`;
- `TELEGRAM_WEBHOOK_MAX_CONNECTIONS=1`;
- `autoDeployTrigger: checksPass`;
- `healthCheckPath: /production/ready`.

`autoDeployTrigger: checksPass` means a commit on the linked production branch is not auto-deployed until its repository CI checks pass. The old manual `autoDeploy: false` release tail is intentionally removed.

`GET /production/ready` is the deployment health authority. It returns `200` only when both conditions are true:

1. MongoDB connectivity is currently usable;
2. Telegram transport is ready. In webhook mode this means the PTB application has successfully registered the webhook, started, and configured the HTTP-to-PTB bridge.

A new Render deploy must not receive production traffic until this health check passes. If it cannot become healthy, the deploy must fail instead of converting a partially initialized process into the live release.

`GET /live` remains a process-liveness endpoint. It is intentionally not the Render deployment gate.

## Pre-deploy Mongo diagnostics

These commands are read-only. Run them from an authorized environment that already has the production `MONGO_URL` when investigating a migration, index warning, or production incident. They do not choose winners or silently mutate production data.

### 1. Legacy Telegram duplicate sessions

```bash
python scripts/check_active_session_duplicates.py
```

Requires at most one `status="in_progress"` quiz session per user. If duplicates exist, stop and resolve them through an explicitly reviewed migration.

### 2. Mini App duplicate open sessions

```bash
python scripts/check_miniapp_session_duplicates.py
```

Requires at most one open Mini App session per user across `in_progress`, `finalizing`, and `score_error`. The command never abandons an arbitrary session or selects a winner.

### 3. Exact unique-index contracts

```bash
python scripts/check_session_unique_indexes.py
```

Checks:

- `quiz_sessions.uniq_active_quiz_user` — unique `user_id`, partial filter `status="in_progress"`;
- `miniapp_sessions.uniq_miniapp_active_user` — unique `user_id`, partial filter over `in_progress`, `finalizing`, `score_error`.

A missing index and an incompatible existing index are both operationally significant. Runtime may create a safe missing index, but it must not silently drop an incompatible guard during rollout.

### 4. Durable-evidence retention / TTL

```bash
python scripts/check_retention_indexes.py
```

Checks terminal-only retention for legacy sessions, Mini App sessions, finalized-and-delivered battles, delivered reports, completed broadcasts, and terminal broadcast recipient rows.

On the first broadcast-aware deploy, only the two explicitly identified broadcast TTL entries may report `bootstrap_pending` with `action=runtime_create_before_http`. Any incompatible or unrecognized TTL remains unsafe. After bootstrap, rerun the command and require no remaining `bootstrap_pending` entries.

### 5. Result-receipt BSON growth and Mongo topology

```bash
python scripts/check_result_storage_growth.py
```

Reports largest leaderboard documents, non-evicting receipt maps, malformed receipt maps, and Mongo topology. Do not delete idempotency receipts merely to make BSON smaller; those receipts prevent replayed results from minting score twice.

## Runtime startup backstops

Before the production HTTP/Telegram composition becomes ready, startup validates the configured Telegram transport and installs/verifies safety-critical session and broadcast indexes. These runtime checks are a backstop, not permission to ignore a known unsafe migration state.

Because Render now gates the deploy on `/production/ready`, a startup failure, Mongo failure, invalid transport, rejected Telegram webhook registration, or webhook bridge that never becomes ready keeps the new deploy unhealthy.

## Telegram webhook diagnostic

For incident investigation or explicit live-state inspection, run from an authorized environment that has `BOT_TOKEN` and either `RENDER_EXTERNAL_URL` or `TELEGRAM_WEBHOOK_BASE_URL`:

```bash
python scripts/check_telegram_webhook.py
```

The command calls only Telegram `getWebhookInfo`. It never calls `setWebhook` or `deleteWebhook`, never changes MongoDB, and never prints `BOT_TOKEN`.

Exit `0` requires:

- the webhook URL equals the expected HTTPS origin plus `/telegram/webhook`;
- `max_connections=1`;
- `allowed_updates` is exactly `message` + `callback_query`;
- no recent Telegram delivery error is present.

A non-zero `pending_update_count` is reported as a warning rather than automatically treated as failure. A continuously growing queue requires investigation.

This script is a diagnostic readback, not a manual deploy trigger and not a recurring human release step.

## Live endpoints

After a release is live, these endpoints have distinct meanings:

- `GET /live` — process is serving HTTP;
- `GET /ready` — MongoDB is reachable;
- `GET /telegram/ready` — configured Telegram transport is ready;
- `GET /production/ready` — MongoDB **and** Telegram transport are both ready; this is Render's health gate;
- `GET /meta` — non-secret Render service/branch/revision identity for incident and revision checks.

Do not substitute `/live` for `/production/ready` in deployment validation.

## Functional smoke scope

Container CI already exercises the production image, controller imports, and a real Mongo container. When performing an explicit end-to-end product acceptance session against the live bot, cover at least:

- `/start` and course catalog;
- normal quiz answers plus timed/speed timeout;
- Challenge 20 and retry-errors;
- report submission;
- PvP create/share/deep-link/join/finish;
- `/status`, restart and cancel;
- one small administrator `/broadcast`, including recovery after a controlled restart.

These product interactions validate user-visible behavior; they are not required to manually unlock each deployment because the automated release gate already owns deploy safety.

## Polling rollback

If webhook delivery itself must be isolated during an incident, change only the transport configuration to:

```text
TELEGRAM_TRANSPORT=polling
```

Redeploy the same application code. Polling uses the same production handlers/state authority. Do not restore legacy `bot.py` as launcher.

`/production/ready` accepts polling rollback only when MongoDB is ready, so the same fail-closed health gate remains active.

## No automatic repair

None of these checks or deployment rules authorizes code to:

- choose an arbitrary duplicate-session winner;
- delete unfinished/finalizing/score-error evidence;
- drop an incompatible unique guard during a rolling deploy;
- delete unfinished broadcast rows to reduce storage;
- clear non-evicting scoring receipts merely to shrink BSON;
- create a self-ping/keepalive loop to defeat hosting sleep behavior;
- mutate Telegram webhook state from the read-only diagnostic script;
- print or commit production credentials.

Any production data/index migration still requires an explicit reviewed plan. Deployment automation removes manual release plumbing; it does not weaken data-safety boundaries.
