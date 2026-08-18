# 📖 bible-bot — Библейский тест по 1 Петра

Telegram-бот и Telegram Mini App для изучения 1-го послания Петра: курсы по главам, исторический контекст, Challenge 20, PvP-битвы, статистика, достижения и лидерборды.

## Production identity

- сайт / индексируемая посадочная: `https://gospod-bog.ru/app/`
- Telegram: `https://t.me/milovanovaibot` (`@milovanovaibot`)
- каноническое имя профиля: **Библейский тренажёр — 1 Петра**

Production startup идемпотентно синхронизирует публичное имя, short description, description и default Menu Button через read/compare/write. Main Mini App / Launch App и его preview media остаются BotFather-owned provider surfaces и проверяются отдельно.

## Архитектура

В проекте один deployable-сервис и один production bootstrap:

```text
Render / Docker
      │
      └─▶ production_entrypoint.py
              │
              ├─ production logging
              └─▶ telegram_production.py  ──▶ focused Telegram controllers
                           │                         │
                           │                         ├─ quiz runtime
                           │                         ├─ courses / retry / challenge
                           │                         ├─ reports / PvP / broadcast
                           │                         └─ settings / stats / admin
                           │
Telegram Bot API ─ webhook ┤
                           └─ polling (explicit transport rollback/local mode)
                                                     │
                                                     └─▶ MongoDB Atlas

Waitress ──▶ /telegram/webhook + Mini App + /api/* + /live + /ready + /production/ready
```

**`production_entrypoint.py` — единственная production-команда запуска.** Он настраивает production logging до импорта Telegram composition root и затем вызывает `telegram_production.main()`.

**`telegram_production.py` — единственный production Telegram composition root.** Он собирает focused controllers и canonical `telegram_quiz_runtime_controller.py`. Исторические runtime-монолиты удалены и не являются compatibility surface, rollback path или источником product authority.

MongoDB остаётся durable authority для quiz/session/result, report, PvP и других persisted state contracts. Process-local runtime state используется только там, где это допустимо UI/runtime-моделью, и не заменяет durable CAS/idempotency boundaries.

Render production использует custom webhook ingress через существующий Flask/Waitress сервер. Webhook проверяет `X-Telegram-Bot-Api-Secret-Token` до JSON parsing и передаёт валидный Telegram `Update` в `Application.update_queue`.

Локально транспорт по умолчанию — `polling`. Это же транспортный rollback: `TELEGRAM_TRANSPORT=polling` меняет способ доставки updates, **не** handler graph и не state authority.

Старый импорт `from intro import ...` поддерживается маленьким `intro.py`, который только реэкспортирует данные из `questions.intro`; вопросы не дублируются.

## Быстрый старт

```bash
cp .env.example .env
# обязательно заполни BOT_TOKEN, ADMIN_USER_ID, MONGO_URL
# BOT_USERNAME нужен для Mini App/bot-info UI и PvP share links
pip install -r requirements-dev.txt
pytest -q
python production_entrypoint.py
```

`.env.example` оставляет `TELEGRAM_TRANSPORT=polling` для локальной разработки. Для webhook вне Render задай HTTPS origin через `TELEGRAM_WEBHOOK_BASE_URL`; на Render используется `RENDER_EXTERNAL_URL`.

Production startup fail-closed проверяет обязательную конфигурацию, Telegram transport и safety-critical storage/index contracts до того, как сервис считается готовым.

После запуска:

- production landing: `https://gospod-bog.ru/app/`
- production bot: `https://t.me/milovanovaibot`
- локальная Mini App: `http://localhost:8080/`
- liveness: `http://localhost:8080/live`
- Mongo readiness: `http://localhost:8080/ready`
- Telegram readiness: `http://localhost:8080/telegram/ready`
- production readiness: `http://localhost:8080/production/ready`
- агрегированная статистика: `http://localhost:8080/stats`

`.env` загружается через `python-dotenv`. Реальный `.env` исключён из Git.

## Telegram webhook

Production route: `POST /telegram/webhook`.

Контракт:

- polling mode скрывает route ответом `404`;
- missing/wrong Telegram secret → `401` до разбора JSON;
- non-JSON → `415`;
- malformed JSON / malformed Telegram Update → `400`;
- PTB bridge ещё не готов во время cold start → retryable `503`;
- update принят в PTB queue → `200`;
- ответы webhook получают `Cache-Control: no-store`;
- `setWebhook` использует только production update types: `message` и `callback_query`;
- `max_connections=1` в Render для детерминированного single-process ingress;
- webhook остаётся зарегистрированным при обычном shutdown/sleep, чтобы следующий Telegram POST мог разбудить Render Free.

Если `TELEGRAM_WEBHOOK_SECRET` не задан, стабильный допустимый secret выводится из `BOT_TOKEN`. При плановой ротации токена можно заранее задать отдельный стабильный secret.

## Telegram Mini App

После деплоя укажи `BOT_USERNAME` и HTTPS-адрес сервиса. Production startup синхронизирует default private-chat Menu Button на canonical Mini App URL только при несовпадении provider state. Main Mini App / профильная **Launch App**-кнопка и preview media настраиваются в `@BotFather`; postdeploy acceptance отдельно требует `getMe.has_main_web_app=true`.

Тот же startup job синхронизирует canonical public bot name, short description и description для default и `ru` locale через read/compare/write. Уже корректный provider state не переписывается.

Клиент использует Telegram CSS variables для цветов и safe-area. Для `prefers-reduced-motion` отключаются лишние анимации.

### Безопасность Mini App API

- сервер проверяет HMAC подпись `Telegram.WebApp.initData`;
- проверяется свежесть `auth_date`;
- production API не принимает `?user_id=...` как аутентификацию;
- quiz POST endpoints принимают только `application/json`;
- quiz/profile/leaderboard API имеют per-user rate limiting;
- API-ответы получают `Cache-Control: no-store`, `X-Content-Type-Options: nosniff` и `Referrer-Policy: no-referrer`;
- правильные ответы и explanation не выдаются до ответа пользователя;
- клиент не отправляет доверенные `score`, `total_points` или время;
- размер теста задаёт сервер: 10 обычный / 20 Challenge;
- повтор старого ответа идемпотентно возвращает сохранённый результат и не двигает сессию;
- новый тест не может обойти MongoDB open-session uniqueness;
- Mini App open-session contract включает `in_progress`, `finalizing`, `score_error`;
- retention применяется только к terminal Mini App sessions (`finished | abandoned`);
- leaderboard с именами пользователей доступен только после Telegram-аутентификации;
- финальная запись статистики проверяется чтением MongoDB.

Development-only header `X-Debug-User-Id` работает только когда одновременно выставлены:

```text
APP_ENV=development
ALLOW_DEBUG_AUTH=true
```

Не включай это в production.

### HTTP resource limits

```text
MAX_REQUEST_BODY_BYTES=1048576
MINIAPP_MAX_REQUEST_BODY_BYTES=65536
MAX_REQUEST_HEADER_BYTES=65536
```

1 MiB — bounded envelope для Telegram webhook Update JSON. Mini App quiz POSTs ограничены 64 KiB на уровне Flask request.

## API

Основной поток Mini App:

```text
POST /api/quiz/start
  { pool_key, mode, count, challenge }

POST /api/quiz/current
  { session_id }

POST /api/quiz/answer
  { session_id, question_id, chosen }
```

`/api/quiz/start` и `/api/quiz/current` возвращают только текущий вопрос без ответа и объяснения. `/api/quiz/answer` возвращает результат проверки после серверной валидации; replay идемпотентен.

Дополнительно:

- `GET /api/me` — Telegram auth
- `GET /api/leaderboard?cat=general|context|hard` — Telegram auth
- `GET /api/pools`
- `GET /api/botinfo`
- `GET /api/questions/<pool>` — compatibility/read-only endpoint без ответов

## Деплой

Перед rollout пройди read-only runbook: **[`docs/DEPLOYMENT_PREFLIGHTS.md`](docs/DEPLOYMENT_PREFLIGHTS.md)**.

Для полного production acceptance используй **[`docs/PRODUCTION_ACCEPTANCE.md`](docs/PRODUCTION_ACCEPTANCE.md)**. CI admission и live production acceptance — разные уровни доказательства.

### Render

`render.yaml` создаёт один Web Service (`numInstances: 1`) и запускает:

```text
python production_entrypoint.py
```

Render production использует `TELEGRAM_TRANSPORT=webhook`, `TELEGRAM_WEBHOOK_MAX_CONNECTIONS=1`, `healthCheckPath: /production/ready` и `autoDeployTrigger: checksPass`.

Free Render Web Service может засыпать при отсутствии входящего трафика. Webhook нужен, чтобы следующий Telegram update был входящим HTTP-запросом и мог разбудить сервис; self-ping keepalive для обхода этой модели не используется.

### Polling rollback

Если нужно изолировать проблему webhook-доставки, меняется только:

```text
TELEGRAM_TRANSPORT=polling
```

и redeploy выполняется с тем же `production_entrypoint.py`. Rollback transport не возвращает старую runtime-архитектуру.

### Docker

```bash
docker build -t bible-bot .
docker run --env-file .env -p 8080:8080 bible-bot
```

Docker image запускает `production_entrypoint.py`. Transport определяется конфигурацией окружения.

## Структура

- `production_entrypoint.py` — единственный process bootstrap для production запуска
- `telegram_production.py` — единственный Telegram composition root
- `telegram_quiz_runtime_controller.py` — canonical quiz runtime/UI controller; durable writes проходят через integrity/session primitives
- `telegram_quiz_runtime_state.py` — process-local quiz runtime state, не durable authority
- `telegram_course_surface.py` — catalog-backed learning/course routing
- `telegram_challenge_controller.py` — Challenge и attempt-bound restart
- `telegram_retry_controller.py` + `legacy_retry_source.py` — restart-safe retry-error practice
- `telegram_report_controller.py` — durable report acceptance/outbox UI adapter
- `telegram_battle_controller.py` — durable PvP progress/finalization/delivery adapter
- `telegram_battle_share_controller.py` — exact-id PvP sharing/deep-link join
- `telegram_broadcast_controller.py` — durable broadcast control
- `telegram_settings_controller.py` — settings surface
- `telegram_admin_controller.py` — recovery-safe production admin operations
- `telegram_public_profile.py` — canonical Telegram public identity and idempotent profile reconciliation
- `web_api/telegram_transport.py` — polling/webhook lifecycle и Waitress→PTB queue bridge
- `database.py` — MongoDB primitives и compatibility storage helpers
- `session_integrity.py`, `battle_integrity.py`, `report_integrity.py` — durable safety boundaries
- `legacy_*.py` — узкие compatibility/protocol/data-format modules; префикс `legacy_` сам по себе не означает executable legacy runtime
- `questions/` — канонические данные вопросов
- `keep_alive.py` — Waitress/HTTP lifecycle внутри процесса
- `web_api/` — auth, HTTP routes, rate limiting, Mini App DB invariants и server-authoritative quiz API
- `miniapp/` — HTML/CSS/JS клиент
- `scripts/check_*.py` — read-only deployment/data-safety/provider preflights
- `utils.py` — PNG/GIF результатов
- `tests/` — behavior, authority, storage, deploy и regression contracts
- `.github/workflows/ci.yml` — actionlint/dependency/secret guards, Ruff, compile, pytest, Mini App JS, production Docker/import/Mongo smoke
- `docs/DEPLOYMENT_PREFLIGHTS.md` — pre/post-deploy operational contract

Исторические executable runtime-монолиты удалены. Возврат второго Telegram application/composition root запрещается retirement regression и production import-graph fence.

## Важный принцип данных

`questions/` — единственный источник истины для вопросов. Если backend недоступен, Mini App показывает ошибку и не подделывает offline result.

Durable scoring/result receipts не являются обычным cache: они предотвращают повторное начисление при crash/retry replay. Не очищай их вручную без отдельной миграционной модели идемпотентности.

## Обновление зависимостей

Dependabot еженедельно проверяет pip и GitHub Actions. Major upgrade `python-telegram-bot` должен оставаться отдельной миграционной волной с расширенными stateful-runtime compatibility tests.

## Проверка перед релизом

Локальные code checks:

```bash
python -m compileall -q .
pytest -q
node --check miniapp/app.js
```

CI дополнительно проверяет maintained Python layer, Mini App JavaScript, Security Audit, CodeQL, production Docker build, built-image production import и Mongo-backed container E2E. Production rollout после merge проверяется отдельным acceptance runbook.
