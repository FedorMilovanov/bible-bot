# Research WAVE 8 — Render-friendly Telegram webhook transport

Date: 2026-08-10

## Goal

Move Telegram update delivery from long polling to a custom webhook transport suitable for a Free Render web service **without mixing the transport migration into the already-green production-hardening PR**.

This wave lives in the stacked branch/PR `agent/bible-bot-webhook-transport`, based on `agent/bible-bot-production-repair`.

## Architecture

The application keeps **one public HTTP server**: the existing Flask application served by Waitress.

Webhook mode does **not** start PTB's optional webhook web server and therefore does not add the `python-telegram-bot[webhooks]` dependency. Instead:

1. Render starts the existing `python bot.py` service.
2. `keep_alive()` starts Waitress on Render's public port.
3. `bot.py` builds the same PTB `Application`, handlers and JobQueue as before.
4. The final launcher delegates to `run_telegram_application(...)`.
5. In webhook mode, the runner initializes PTB and registers the Telegram webhook.
6. PTB `Application.start()` starts the update processor and JobQueue.
7. Only **after** PTB is running does the bridge become ready for HTTP submissions.
8. `POST /telegram/webhook` validates Telegram's secret header before parsing JSON.
9. Valid Telegram `Update` JSON is decoded with PTB and forwarded from the Waitress thread into `Application.update_queue` with `asyncio.run_coroutine_threadsafe`.
10. On shutdown the bridge first stops accepting new submissions, drains already-started submissions with a bounded timeout, stops PTB, and only then persists the in-memory quiz sessions.

This follows PTB's documented custom-webhook architecture while preserving the large stateful handler graph.

## Render behavior and why webhook is useful

Render Free web services can spin down when they receive no inbound traffic. Polling is outbound traffic from the bot process and therefore is not a useful wake-up mechanism for a sleeping web service. A Telegram webhook is an inbound HTTPS request to the Render service, so Telegram activity can wake the service.

The webhook URL defaults to Render's built-in `RENDER_EXTERNAL_URL`; no duplicated hard-coded hostname is required.

## Webhook security

### Secret token

The ingress verifies `X-Telegram-Bot-Api-Secret-Token` before parsing the request body.

An explicit `TELEGRAM_WEBHOOK_SECRET` is supported only if it matches `[A-Za-z0-9_-]{16,256}`. If no explicit secret is configured, the service deterministically derives a 64-character hexadecimal secret from `BOT_TOKEN`. This avoids adding another mandatory production secret while remaining inside Telegram's allowed character set.

For a planned `BOT_TOKEN` rotation, an explicit stable `TELEGRAM_WEBHOOK_SECRET` is preferable: the derived secret changes with the bot token. Without an explicit secret there can be a short transition window where the old webhook delivery receives a non-2xx response until the new process successfully re-registers the webhook. Telegram retries unsuccessful webhook deliveries.

### Fail-closed responses

- polling mode: webhook route returns `404`;
- invalid/missing secret: `401`;
- non-JSON: `415`;
- malformed/non-object JSON: `400`;
- malformed Telegram Update or invalid/missing `update_id`: `400`;
- PTB bridge not ready during cold start or shutdown: `503`, allowing Telegram to retry;
- queue submission failure: `503`;
- accepted queue submission: `200`.

Webhook responses are `no-store`.

## PTB lifecycle

Webhook mode uses the PTB `Application` lifecycle directly:

1. initialize through the async application context;
2. register the webhook and require `setWebhook` to return `True`;
3. call `Application.start()` so the PTB update processor and JobQueue are active;
4. expose the Waitress-to-PTB bridge only after start succeeds;
5. wait for SIGINT/SIGTERM;
6. deactivate the bridge so new webhook requests receive retryable `503`;
7. wait up to 3 seconds for submissions that had already crossed the HTTP boundary to finish their queue handoff;
8. call `Application.stop()`;
9. run the existing `_save_all_sessions` hook **after** PTB has stopped changing application state;
10. exit the application context and complete PTB shutdown.

The webhook is intentionally **not deleted on normal Render shutdown/sleep**. Keeping it registered allows the next Telegram HTTP request to reach and wake the sleeping service.

PTB documents that `Application.stop()` no longer fetches new updates from `update_queue` once stop begins. The drain barrier therefore closes a real boundary race: a Waitress request that had passed readiness immediately before shutdown must finish `queue.put` before PTB is stopped.

## Telegram update allowlist

The legacy handler graph was inspected directly. It registers handlers for:

- messages (commands, text and report photos);
- callback queries;
- inline queries.

It does not register ChatMember, poll, shipping, pre-checkout or generic `TypeHandler(Update, ...)` handlers.

The webhook therefore requests only:

- `message`;
- `callback_query`;
- `inline_query`.

This avoids asking Telegram to deliver update classes the application would ignore.

## Polling rollback and legacy signal ownership

`TELEGRAM_TRANSPORT=polling` delegates to the original PTB `application.run_polling()` behavior. PTB's polling bootstrap removes an existing webhook before starting `getUpdates`, so rollback does not require a separate manual webhook deletion command.

Telegram itself does not allow `getUpdates` while a webhook is configured, which is why transport selection is explicit rather than simultaneous.

`bot.py` still contains the older SIGTERM/SIGINT handler used to run `_save_all_sessions()` in polling mode. In webhook mode the asyncio transport owns the Unix signals and performs the stronger ordered shutdown above. The legacy signal block is deliberately not deleted in this wave because doing so without migrating polling shutdown persistence to PTB `post_stop` would weaken the rollback path. A future cleanup can move polling persistence to `post_stop` and then remove the old signal handler in one atomic change.

## Delivery concurrency

Telegram's Bot API supports `max_connections` from 1 to 100 and defaults to 40. Lower values limit server load.

The deployment explicitly sets:

- `WEB_THREADS=4`;
- `TELEGRAM_WEBHOOK_MAX_CONNECTIONS=4`.

The transport validates the 1-100 Bot API range before registration and passes the configured value to `setWebhook`.

## Request-size split

The original hardening baseline limited every HTTP request body to 64 KiB. Telegram does not publish a guarantee that every future webhook Update will stay below that value, so webhook transport must not accidentally inherit a Mini App-specific assumption.

The transport branch now uses two limits:

- **server/Waitress envelope:** 1 MiB via `MAX_REQUEST_BODY_BYTES=1048576`;
- **Mini App quiz POSTs:** 64 KiB via `MINIAPP_MAX_REQUEST_BODY_BYTES=65536` and Flask 3.1 per-request `Request.max_content_length`.

Regression tests prove that a JSON webhook payload above 64 KiB but below the server envelope reaches the bridge, while an oversized Mini App quiz request still returns `413`.

Headers remain capped at 64 KiB.

## Launcher safety

`bot.py` is a large legacy file and was deliberately **not** manually rewritten through the GitHub Contents API.

The launcher replacement was applied by a one-shot guarded CI job using an exact byte replacement. The job required:

- exactly one CRLF `app.run_polling()` byte sequence;
- exactly one new transport-launch sequence after replacement;
- reverse replacement to reproduce the original `bot.py` bytes exactly;
- Git numstat exactly `+2/-1` for `bot.py`;
- no other file change in that patch commit.

GitHub compare then confirmed that launcher commit changed only `bot.py` by `+2/-1`. The one-shot write workflow was deleted immediately afterwards and is not part of the final transport diff.

## Test coverage added in this wave

Transport tests cover:

- polling default and polling rollback delegation;
- invalid transport mode;
- webhook URL derivation and HTTPS validation;
- webhook secret derivation and validation;
- webhook connection-limit validation;
- exact Telegram update-type allowlist;
- real minimal Telegram `Update` decoding and queue submission;
- rejection of missing, boolean or negative `update_id` values;
- PTB bridge remaining unavailable until `Application.start()` has succeeded;
- bridge deactivation rejecting new submissions while draining an already-started submission;
- shutdown order `deactivate/drain -> Application.stop() -> save sessions -> Application.shutdown()`;
- startup failure when Telegram rejects `setWebhook`;
- webhook route authentication before JSON parsing;
- invalid JSON / invalid update handling;
- cold-start bridge `503`;
- successful forwarding;
- webhook body above the Mini App 64 KiB limit;
- Render deployment contract;
- exact bot launcher contract;
- no PTB webhook-extra dependency.

The standard branch CI additionally runs the entire inherited hardening suite, Node Mini App tests, Docker build and built-container runtime smoke. PyPA `pip-audit` remains an independent gate. Stacked PRs additionally run Python and JavaScript/TypeScript CodeQL through `.github/workflows/codeql-stacked.yml`.

## Delivery semantics / accepted residual risk

The official PTB custom-webhook pattern places a validated update in `Application.update_queue` and returns a successful HTTP response. This branch follows that model.

A successful `200` therefore means the update was accepted into the **in-memory PTB queue**, not durably committed to an external broker. There is a very small crash window between HTTP acknowledgement and handler completion. Adding Mongo/Kafka/Redis-backed webhook ingestion now would materially increase architecture and failure modes for a small single-process bot, so it is deliberately not added without production evidence that durable ingress is needed.

The new shutdown drain closes the separate race between an in-flight Waitress submission and `Application.stop()`; it does not attempt to turn the PTB in-memory queue into durable storage.

Telegram `update_id` is suitable for detecting repeated webhook updates. If real duplicate-handler side effects appear under production delivery/retry conditions, bounded deduplication can be added as an isolated follow-up without changing the transport topology.

## Primary sources

- Telegram Bot API — `setWebhook`, retry behavior, secret token, `max_connections`, `allowed_updates`: https://core.telegram.org/bots/api#setwebhook
- Telegram Bot API — `Update.update_id`: https://core.telegram.org/bots/api#update
- python-telegram-bot 20.7 — custom webhook example: https://docs.python-telegram-bot.org/en/v20.7/examples.customwebhookbot.html
- python-telegram-bot 20.7 — `Application.start()` / `Application.stop()` / update queue processing: https://docs.python-telegram-bot.org/en/v20.7/telegram.ext.application.html
- python-telegram-bot 20.7 — polling lifecycle and `post_stop`: https://docs.python-telegram-bot.org/en/v20.7/telegram.ext.application.html#telegram.ext.Application.run_polling
- python-telegram-bot 20.7 — `Bot.set_webhook`: https://docs.python-telegram-bot.org/en/v20.7/telegram.bot.html#telegram.Bot.set_webhook
- Render — Free web services: https://render.com/docs/free
- Render — default environment variables / `RENDER_EXTERNAL_URL`: https://render.com/docs/environment-variables#default-environment-variables
- Flask 3.1 — request-size limits / per-request `Request.max_content_length`: https://flask.palletsprojects.com/en/stable/web-security/#resource-use

## Remaining work before merge/deploy

All repository-side gates are green on the latest transport code before this documentation update: CI, Security Audit, stacked CodeQL, Docker build/runtime smoke and the inherited hardening suite.

The remaining blocker is the **real Render + Telegram smoke using production secrets**:

1. deploy the stacked branch;
2. confirm `/live` is healthy;
3. inspect Telegram `getWebhookInfo`;
4. send `/start`, callback buttons, quiz answers, inline query, report flow and Challenge 20 through Telegram;
5. verify cold-start delivery after the Free service has slept;
6. verify Mongo/session continuity across restart;
7. verify shutdown/redeploy does not delete the webhook;
8. test rollback by switching `TELEGRAM_TRANSPORT=polling` and confirm polling resumes and sessions still save.

Secrets must not be committed or pasted into the PR.
