# 📖 bible-bot — Библейский тест по 1 Петра

Telegram-бот и Telegram Mini App для изучения 1-го послания Петра: вопросы по главе 1, исторический контекст, Challenge 20, PvP-битвы, статистика, достижения и лидерборды.

## Архитектура

В проекте один deployable-сервис:

```text
Telegram Bot API ── webhook ──▶ Waitress /telegram/webhook
                                  │
                                  └─▶ PTB update_queue ──▶ telegram_production.py
                                                           │
                                                           ├─ durable quiz / retry / report / PvP adapters
                                                           │                  │
                                                           │                  └─ MongoDB Atlas
                                                           │
                                                           └─ transitional bot.py presentation helpers

Telegram Bot API ◀── polling ── telegram_production.py   (explicit rollback/local mode)

Waitress ──▶ Mini App + /api/* + /live + /ready
```

**`telegram_production.py` — единственный production Telegram composition root.** Он регистрирует Mongo-authoritative quiz/retry/report/PvP handlers и не регистрирует исторические state-writers из `bot.py`. `bot.py` остаётся transitional compatibility/presentation library для ещё не вынесенных меню, статистики и вспомогательных действий.

Render production использует custom webhook ingress через существующий Flask/Waitress сервер. Отдельный PTB webhook-server и dependency `python-telegram-bot[webhooks]` не нужны. Webhook проверяет `X-Telegram-Bot-Api-Secret-Token` до JSON parsing и передаёт валидный Telegram `Update` в `Application.update_queue`.

Локально транспорт по умолчанию остаётся `polling`. Это же явный rollback-путь: `TELEGRAM_TRANSPORT=polling` возвращает `Application.run_polling()` без изменения handler graph.

Старый импорт `from intro import ...` поддерживается маленьким `intro.py`, который только реэкспортирует данные из `questions.intro`; вопросы не дублируются.

Mini App не доверяет клиенту результаты квиза: сервер создаёт сессию, хранит правильные ответы, проверяет каждый `question_id`, считает score/time/bonus и только после этого пишет результат в MongoDB.

## Быстрый старт

```bash
cp .env.example .env
# обязательно заполни BOT_TOKEN, ADMIN_USER_ID, MONGO_URL
# BOT_USERNAME нужен для Mini App/bot-info UI и PvP share links
pip install -r requirements-dev.txt
pytest -q
python telegram_production.py
```

`.env.example` оставляет `TELEGRAM_TRANSPORT=polling` для локальной разработки. Для webhook вне Render задай HTTPS origin через `TELEGRAM_WEBHOOK_BASE_URL`; на Render используется автоматически предоставляемый `RENDER_EXTERNAL_URL`.

Production startup fail-closed проверяет `BOT_TOKEN`, `MONGO_URL`, обязательный JobQueue и safety-critical session indexes до начала Telegram transport. Отсутствующий или несовместимый storage contract не должен превращаться в «здоровый», но фактически недолговечный бот.

После запуска:

- бот: `https://t.me/<BOT_USERNAME>`
- Mini App: `http://localhost:8080/`
- liveness: `http://localhost:8080/live`
- readiness MongoDB: `http://localhost:8080/ready`
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
- `setWebhook` использует только реально поддерживаемые production update types: `message` и `callback_query`;
- `max_connections=1` в Render для детерминированного single-process ingress;
- webhook остаётся зарегистрированным при обычном shutdown/sleep, поэтому следующий Telegram POST может разбудить Render Free.

Если `TELEGRAM_WEBHOOK_SECRET` не задан, стабильный допустимый secret выводится из `BOT_TOKEN`. При плановой ротации токена можно заранее задать отдельный стабильный secret.

## Telegram Mini App

После деплоя укажи `BOT_USERNAME` и HTTPS-адрес сервиса. В `@BotFather` настрой **Menu Button** на этот HTTPS URL — это основной нативный вход в Mini App.

Клиент использует официальные Telegram CSS variables для цветов и safe-area, поэтому интерфейс адаптируется к теме Telegram и вырезам/системным панелям устройства. Для `prefers-reduced-motion` отключаются лишние анимации.

### Безопасность Mini App API

- сервер проверяет HMAC подпись `Telegram.WebApp.initData`;
- проверяется свежесть `auth_date` (`TELEGRAM_INIT_DATA_MAX_AGE_SECONDS`);
- размер `initData` ограничен до HMAC/URL parsing, а уже проверенный пользователь кэшируется на время одного HTTP-запроса;
- production API не принимает `?user_id=...` и другие подмены пользователя;
- quiz POST endpoints принимают только `application/json`;
- quiz/profile/leaderboard API имеют per-user rate limiting; при превышении возвращается `429` + `Retry-After`;
- API-ответы получают `Cache-Control: no-store`, `X-Content-Type-Options: nosniff` и `Referrer-Policy: no-referrer`;
- правильные ответы и explanation не выдаются до ответа пользователя;
- клиент не отправляет `score`, `total_points` или доверенное время;
- размер теста задаёт сервер: 10 обычный / 20 Challenge;
- сервер не начинает тест, если выбранный пул физически меньше требуемого размера;
- повтор старого ответа идемпотентно возвращает уже сохранённый результат и не двигает сессию;
- новый тест не может обойти MongoDB open-session uniqueness;
- Mini App open-session contract включает `in_progress`, `finalizing`, `score_error` — эти состояния защищены одним partial unique index на пользователя;
- исторический generic TTL не имеет права удалять open/recovery states; retention применяется только к terminal Mini App sessions (`finished | abandoned`);
- leaderboard с именами пользователей доступен только после Telegram-аутентификации;
- Mini App сессии хранятся отдельно от Telegram-bot quiz sessions;
- финальная запись статистики проверяется чтением MongoDB; при неподтверждённой записи сервер не показывает выдуманные баллы.

Development-only header `X-Debug-User-Id` работает только когда одновременно выставлены:

```text
APP_ENV=development
ALLOW_DEBUG_AUTH=true
```

Не включай это в деплое.

### HTTP resource limits

Production разделяет внешний server envelope и Mini App quiz payload:

```text
MAX_REQUEST_BODY_BYTES=1048576
MINIAPP_MAX_REQUEST_BODY_BYTES=65536
MAX_REQUEST_HEADER_BYTES=65536
```

1 MiB нужен только как bounded envelope для Telegram webhook Update JSON. Mini App quiz POSTs по-прежнему ограничены 64 KiB на уровне конкретного Flask request.

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

`/api/quiz/start` и `/api/quiz/current` возвращают только **один текущий** вопрос без ответа и объяснения. Будущие вопросы браузеру заранее не выдаются. `/api/quiz/answer` возвращает правильный индекс и объяснение уже после серверной проверки; повтор того же HTTP-ответа идемпотентен и не начисляет очки повторно.

Дополнительно:

- `GET /api/me` — Telegram auth
- `GET /api/leaderboard?cat=general|context|hard` — Telegram auth
- `GET /api/pools`
- `GET /api/botinfo`
- `GET /api/questions/<pool>` — legacy/read-only endpoint без ответов

## Деплой

Перед rollout обязательно пройди постоянный read-only runbook: **[`docs/DEPLOYMENT_PREFLIGHTS.md`](docs/DEPLOYMENT_PREFLIGHTS.md)**.

Он отдельно проверяет:

1. duplicate `in_progress` Telegram quiz sessions;
2. duplicate open Mini App sessions;
3. exact unique-index contracts обоих session stores;
4. terminal-only retention/TTL для Telegram sessions, Mini App, battles и reports;
5. BSON/result-receipt growth и Mongo topology.

Preflight-команды не удаляют строки и не чинят индексы автоматически. Unsafe (`exit 1`) и unavailable (`exit 2`) требуют остановить rollout и разобраться до deploy.

### Render

`render.yaml` создаёт **один** Free Web Service (`numInstances: 1`) и запускает:

```text
python telegram_production.py
```

Render production устанавливает `TELEGRAM_TRANSPORT=webhook`. `autoDeploy` намеренно выключен. `/live` остаётся shallow liveness, `/ready` — Mongo-aware readiness.

Free Render Web Service засыпает при отсутствии входящего HTTP/WebSocket трафика. Polling — исходящий трафик и не решает эту модель. Webhook нужен именно затем, чтобы следующий Telegram update был входящим HTTP-запросом, разбудил сервис, а Telegram повторил delivery при временном non-2xx во время cold start. Никаких self-ping keepalive jobs для этого не требуется.

После deploy проверь webhook/cold-start пункты из `docs/DEPLOYMENT_PREFLIGHTS.md`.

### Docker

```bash
docker build -t bible-bot .
docker run --env-file .env -p 8080:8080 bible-bot
```

Docker image запускает `telegram_production.py`. Если `.env` оставляет `TELEGRAM_TRANSPORT=polling`, локальный Docker работает в polling rollback mode. Для webhook задай HTTPS public origin отдельно.

## Структура

- `telegram_production.py` — единственный production Telegram composition root
- `telegram_controller.py` — Mongo-authoritative quiz lifecycle/result controller
- `telegram_retry_controller.py` + `legacy_retry_source.py` — restart-safe retry-error practice
- `telegram_report_controller.py` — durable report acceptance/outbox UI adapter
- `telegram_battle_controller.py` — durable PvP progress/finalization/delivery adapter
- `telegram_battle_share_controller.py` — exact-id PvP sharing/deep-link join
- `telegram_admin_controller.py` — recovery-safe production admin cleanup
- `web_api/telegram_transport.py` — polling/webhook lifecycle и Waitress→PTB queue bridge
- `bot.py` — transitional legacy presentation/read/process-local helpers; не production state authority
- `database.py` — MongoDB, статистика и compatibility storage helpers
- `questions/` — канонические данные вопросов
- `keep_alive.py` — lifecycle Waitress внутри процесса
- `web_api/` — auth, HTTP routes, rate limiting, Mini App DB invariants и server-authoritative quiz API
- `miniapp/` — HTML/CSS/JS клиент
- `scripts/check_*.py` — read-only deployment/data-safety preflights
- `utils.py` — PNG/GIF результатов
- `tests/` — API/auth/session/hardening/regression contracts
- `.github/workflows/ci.yml` — actionlint/dependency/secret guards, Ruff, compile, pytest, Mini App JS, production Docker/import/smoke
- `.github/dependabot.yml` — контролируемые dependency updates
- `docs/DEPLOYMENT_PREFLIGHTS.md` — обязательный pre/post-deploy runbook
- `docs/RESEARCH_WAVE*.md` — research/integrity trail предыдущих волн

## Важный принцип данных

`questions/` — единственный источник истины для вопросов. Дублирующий `miniapp/demo_questions.json` удалён: он быстро расходился бы с основной базой и позволял клиенту видеть правильные ответы. Если backend недоступен, Mini App показывает ошибку и не подделывает «офлайн-результат».

Durable scoring/result receipts intentionally не являются обычным cache: они предотвращают повторное начисление при replay после crash/retry. Не очищай их вручную только ради уменьшения документа без отдельной миграционной модели идемпотентности.

## Обновление зависимостей

Dependabot еженедельно проверяет pip и GitHub Actions. Minor/patch Python updates группируются. Major upgrade `python-telegram-bot` намеренно не автоматизирован: переход с 20.7 на текущую major-ветку должен быть отдельной миграционной волной с расширенными compatibility-тестами stateful bot runtime.

## Проверка перед релизом

Локальные code checks:

```bash
python -m compileall -q .
pytest -q
node --check miniapp/app.js
```

Перед production rollout дополнительно выполни все команды из [`docs/DEPLOYMENT_PREFLIGHTS.md`](docs/DEPLOYMENT_PREFLIGHTS.md) в авторизованной среде с production `MONGO_URL`.

CI также проверяет maintained Python layer, full pytest, Mini App JavaScript, Security Audit, CodeQL, production Docker build, built-image production import и `/live` smoke.
