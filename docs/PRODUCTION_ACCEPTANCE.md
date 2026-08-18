# Production acceptance gate

This repository reaches **100% production acceptance** only when both phases below exit `0` against the exact deployed revision. CI alone is not production acceptance.

The runner is read-only. It does not merge, deploy, mutate MongoDB, call Telegram `setWebhook`/`deleteWebhook`, change BotFather/Main Mini App provider state, change the public bot profile, expose secrets, or create temporary workflows.

## Phase 1 — before deploy

Run from an authorized environment that already contains the production `MONGO_URL`:

```bash
python scripts/run_production_acceptance.py predeploy
```

Exit `0` requires all five production Mongo checks to be safe:

1. active Telegram quiz-session duplicates;
2. Mini App open-session duplicates;
3. exact Telegram + Mini App unique-index contracts;
4. durable-evidence retention / TTL contracts;
5. result-receipt BSON growth and Mongo topology.

Exit `1` means an external contract is unsafe and requires operator review. Exit `2` means the contract could not be established. Neither is permission to auto-repair production data.

## Deploy boundary

Only after phase 1 is green, make the explicitly authorized merge/deploy decision. `render.yaml` remains the deployment authority and must keep the reviewed single-instance, webhook, single-connection and `autoDeployTrigger: checksPass` contract unless a separately reviewed architecture change intentionally replaces it.

Checks-gated auto-deploy does not remove the merge authorization boundary: it means an authorized commit on the linked production branch is deployable only after repository checks pass.

Record the exact 40-hex revision actually deployed. Do not infer it from branch names.

The production startup owns one intentionally mutable public Telegram surface in addition to the existing command/Menu Button sync: the canonical bot name, short description and description for the default and `ru` locales. That reconciliation is read/compare/write and therefore performs **zero public-profile writes when provider state already matches the canonical contract**. It does not own Main Mini App profile/media configuration, webhook state or any other BotFather surface.

## Phase 2 — after deploy

Run from an authorized environment containing `BOT_TOKEN` and either `RENDER_EXTERNAL_URL` or `TELEGRAM_WEBHOOK_BASE_URL`. Set `EXPECTED_DEPLOY_SHA` to the exact 40-hex revision intended to be live:

```bash
EXPECTED_DEPLOY_SHA=<40-hex-revision> \
python scripts/run_production_acceptance.py postdeploy
```

Exit `0` requires all of the following:

- `/live` returns HTTP 200 JSON with `status=ok`;
- `/ready` returns HTTP 200 JSON with `status=ready` and `database=true`;
- `/telegram/ready` returns HTTP 200 JSON with `status=ready` and `transport=webhook`;
- `/meta` reports exactly `EXPECTED_DEPLOY_SHA`, preventing a green smoke against an older Render build;
- the retention preflight is green **without** `bootstrap_pending`;
- Telegram `getWebhookInfo` matches the exact URL, single connection and allowed-update contract with no current unsafe delivery error;
- Telegram `getMe` reports `has_main_web_app=true` for the production bot, proving the BotFather-owned Main Mini App / Launch App provider state is enabled;
- Telegram `getMyName`, `getMyShortDescription` and `getMyDescription` match the canonical public profile for the default provider query and the `ru` provider query.

The Main Mini App and public-profile checks are read-only. A reachable mismatch is a known unsafe provider state and returns exit `1`; an unavailable or malformed Telegram response returns exit `2`. The profile checker reports only normalized locale/field mismatch names and never prints `BOT_TOKEN`, provider request URLs or secret-bearing errors.

## Final manual smoke

Machine checks do not replace user-visible behavior. Before calling the rollout 100% accepted, exercise the exact deployed revision through:

- the bot profile `Launch App` action and the default private-chat Menu Button;
- visible bot profile name, short description and full description in Telegram;
- `?startapp=v1_site_app__home` and the reviewed contextual launches `v1_site_ch3__chapter3` / `v1_site_ch4__chapter4`;
- `/start`, normal quiz answer and result;
- timed/speed timeout path;
- Challenge 20;
- retry-errors;
- report submission including photo/text delivery behavior;
- PvP create/share/deep-link/join/finish;
- `/status`, restart and cancel;
- one small durable administrator broadcast;
- controlled application restart with pending report/PvP/broadcast recovery;
- Render Free cold-start wake via a Telegram update.

After the cold-start/recovery smoke, run the postdeploy acceptance runner again and require exit `0`.

Only then is the production rollout **100% accepted**. A green PR, Docker smoke, `/live` alone, or a successful deploy status is not sufficient.
