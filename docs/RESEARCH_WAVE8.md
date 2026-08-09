# Research WAVE 8 — Render-friendly Telegram webhook transport

Date: 2026-08-09

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
5. In webhook mode, the runner initializes PTB, registers the Telegram webhook, starts the PTB application, and leaves Waitress as the only socket owner.
6. `POST /telegram/webhook` validates Telegram's secret header before parsing JSON.
7. Valid Telegram `Update` JSON is decoded with PTB and forwarded from the Waitress thread into `Application.update_queue` with `asyncio.run_coroutine_threadsafe`.
8. PTB's normal update processor and existing handler graph consume the queue.

This follows PTB's documented custom-webhook architecture while preserving the large stateful handler graph.

## Render behavior and why webhook is useful

Render Free web services can spin down when they receive no inbound traffic. Polling is outbound traffic from the bot process and therefore is not a useful wake-up mechanism for a sleeping web service. A Telegram webhook is an inbound HTTPS request to the Render service, so Telegram activity can wake the service.

The webhook URL defaults to Render's built-in `RENDER_EXTERNAL_URL`; no duplicated hard-coded hostname is required.

## Webhook security

### Secret token

The ingress verifies `X-Telegram-Bot-Api-Secret-Token` before parsing the request body.

An explicit `TELEGRAM_WEBHOOK_SECRET` is supported only if it matches `[A-Za-z0-9_-]{16,256}`. If no explicit secret is configured, the service deterministically derives a 64-character hexadecimal secret from `BOT_TOKEN`. This avoids adding another mandatory production secret while remaining inside Telegram's allowed character set.

### Fail-closed responses

- polling mode: webhook route returns `404`;
- invalid/missing secret: `401`;
- non-JSON: `415`;
- malformed/non-object JSON: `400`;
- malformed Telegram Update: `400`;
- PTB bridge not ready during cold start: `503`, allowing Telegram to retry;
- accepted queue submission: `200`.

Webhook responses are `no-store`.

## PTB lifecycle

Webhook mode uses the PTB `Application` lifecycle directly:

- initialize through the async application context;
- register the webhook;
- require `setWebhook` to return `True`, otherwise startup fails;
- `Application.start()` runs PTB's update processor and JobQueue;
- wait for SIGINT/SIGTERM;
- run the existing `_save_all_sessions` shutdown hook;
- `Application.stop()` and application shutdown.

The webhook is intentionally **not deleted on normal Render shutdown/sleep**. Keeping it registered allows the next Telegram HTTP request to reach and wake the sleeping service.

## Polling rollback

`TELEGRAM_TRANSPORT=polling` delegates to the original PTB `application.run_polling()` behavior. PTB's polling bootstrap removes an existing webhook before starting `getUpdates`, so rollback does not require a separate manual webhook deletion command.

Telegram itself does not allow `getUpdates` while a webhook is configured, which is why transport selection is explicit rather than simultaneous.

## Delivery concurrency

Telegram's Bot API supports `max_connections` from 1 to 100 and defaults to 40. PTB recommends reducing it to limit server load.

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
- real minimal Telegram `Update` decoding and queue submission;
- PTB webhook lifecycle and shutdown hook;
- startup failure when Telegram rejects `setWebhook`;
- webhook route authentication before JSON parsing;
- invalid JSON / invalid update handling;
- cold-start bridge `503`;
- successful forwarding;
- webhook body above the Mini App 64 KiB limit;
- Render deployment contract;
- exact bot launcher contract;
- no PTB webhook-extra dependency.

The standard branch CI additionally runs the entire inherited hardening suite, Node Mini App tests, Docker build and built-container runtime smoke. PyPA `pip-audit` remains an independent gate.

## Delivery semantics / accepted residual risk

The official PTB custom-webhook pattern places a validated update in `Application.update_queue` and returns a successful HTTP response. This branch follows that model.

A successful `200` therefore means the update was accepted into the **in-memory PTB queue**, not durably committed to an external broker. There is a very small crash window between HTTP acknowledgement and handler completion. Adding Mongo/Kafka/Redis-backed webhook ingestion now would materially increase architecture and failure modes for a small single-process bot, so it is deliberately not added without production evidence that durable ingress is needed.

Telegram `update_id` is suitable for detecting repeated webhook updates. If real duplicate-handler side effects appear under production delivery/retry conditions, bounded deduplication can be added as an isolated follow-up without changing the transport topology.

## Primary sources

- Telegram Bot API — `setWebhook`, `getUpdates`, secret token, `max_connections`: https://core.telegram.org/bots/api#setwebhook
- Telegram Bot API — `Update.update_id`: https://core.telegram.org/bots/api#update
- python-telegram-bot 20.7 — custom webhook example: https://docs.python-telegram-bot.org/en/v20.7/examples.customwebhookbot.html
- python-telegram-bot 20.7 — `Application.start()` / update queue processing: https://docs.python-telegram-bot.org/en/v20.7/telegram.ext.application.html
- python-telegram-bot 20.7 — `Bot.set_webhook`: https://docs.python-telegram-bot.org/en/v20.7/telegram.bot.html#telegram.Bot.set_webhook
- Render — Free web services: https://render.com/docs/free
- Render — default environment variables / `RENDER_EXTERNAL_URL`: https://render.com/docs/environment-variables#default-environment-variables
- Flask 3.1 — request-size limits / per-request `Request.max_content_length`: https://flask.palletsprojects.com/en/stable/web-security/#resource-use

## Remaining work before merge/deploy

1. Let the final stacked-PR CI and security audit finish on the documentation head.
2. Perform a real Render + Telegram smoke using production secrets:
   - deploy the stacked branch;
   - confirm `/live` is healthy;
   - inspect Telegram `getWebhookInfo`;
   - send `/start`, callback buttons, quiz answers, inline query, report flow and Challenge 20 through Telegram;
   - verify cold-start delivery after the Free service has slept;
   - verify Mongo/session continuity;
   - verify shutdown/redeploy does not delete the webhook.
3. Keep PR #1 mergeable independently. Merge/deploy the transport change only after the external webhook smoke succeeds.
