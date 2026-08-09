# 📖 bible-bot — Библейский тест по 1 Петра

Telegram-бот и Telegram Mini App для изучения 1-го послания Петра: вопросы по главе 1, исторический контекст, Challenge 20, PvP-битвы, статистика, достижения и лидерборды.

## Архитектура

В проекте один deployable-сервис:

```text
Telegram Bot API ← polling ← bot.py
                         │
                         ├─ MongoDB Atlas
                         │
                         └─ keep_alive.py → Waitress → Mini App + /api/*
                                                  └─ miniapp/
```

`bot.py` остаётся источником полной Telegram-логики и сохранён byte-for-byte относительно `main`. При импорте `keep_alive()` запускает production WSGI-сервер Waitress в отдельном потоке. Поэтому `python bot.py` одновременно поднимает бота, Mini App и API — без второго Render worker и без Flask development server.

Старый импорт `from intro import ...` поддерживается маленьким `intro.py`, который только реэкспортирует данные из `questions.intro`; вопросы не дублируются.

Mini App не доверяет клиенту результаты квиза: сервер создаёт сессию, хранит правильные ответы, проверяет каждый `question_id`, считает score/time/bonus и только после этого пишет результат в MongoDB.

## Быстрый старт

```bash
cp .env.example .env
# заполни BOT_TOKEN, ADMIN_USER_ID, MONGO_URL, BOT_USERNAME
pip install -r requirements-dev.txt
pytest -q
python bot.py
```

После запуска:

- бот: `https://t.me/<BOT_USERNAME>`
- Mini App: `http://localhost:8080/`
- liveness: `http://localhost:8080/live`
- readiness MongoDB: `http://localhost:8080/ready`
- агрегированная статистика: `http://localhost:8080/stats`

`.env` загружается через `python-dotenv`. Реальный `.env` исключён из Git.

## Telegram Mini App

После деплоя укажи `BOT_USERNAME` и HTTPS-адрес сервиса. В `@BotFather` настрой **Menu Button** на этот HTTPS URL — это основной нативный вход в Mini App. Legacy `bot.py` специально не переписывается тысячами строк ради ещё одной inline-кнопки.

Клиент использует официальные Telegram CSS variables для цветов и safe-area, поэтому интерфейс адаптируется к теме Telegram и вырезам/системным панелям устройства. Для `prefers-reduced-motion` отключаются лишние анимации.

### Безопасность Mini App API

- сервер проверяет HMAC подпись `Telegram.WebApp.initData`;
- проверяется свежесть `auth_date` (`TELEGRAM_INIT_DATA_MAX_AGE_SECONDS`);
- размер `initData` ограничен до HMAC/URL parsing, а уже проверенный пользователь кэшируется на время одного HTTP-запроса;
- production API не принимает `?user_id=...` и другие подмены пользователя;
- quiz POST endpoints принимают только `application/json`;
- Flask и Waitress ограничивают тело и суммарные заголовки запроса (по умолчанию 64 KiB);
- quiz/profile/leaderboard API имеют per-user rate limiting; при превышении возвращается `429` + `Retry-After`;
- API-ответы получают `Cache-Control: no-store`, `X-Content-Type-Options: nosniff` и `Referrer-Policy: no-referrer`;
- правильные ответы и explanation не выдаются до ответа пользователя;
- клиент не отправляет `score`, `total_points` или доверенное время;
- размер теста задаёт сервер: 10 обычный / 20 Challenge;
- сервер не начинает тест, если выбранный пул физически меньше требуемого размера;
- повтор старого ответа идемпотентно возвращает уже сохранённый результат и не двигает сессию;
- новый тест явно помечает прежнюю незавершённую Mini App-сессию как `abandoned`;
- MongoDB partial unique index дополнительно гарантирует максимум одну `in_progress` Mini App-сессию на пользователя;
- leaderboard с именами пользователей доступен только после Telegram-аутентификации;
- Mini App сессии хранятся отдельно от Telegram-bot quiz sessions и имеют TTL;
- финальная запись статистики проверяется чтением MongoDB; при неподтверждённой записи сервер не показывает выдуманные баллы.

Development-only header `X-Debug-User-Id` работает только когда одновременно выставлены:

```text
APP_ENV=development
ALLOW_DEBUG_AUTH=true
```

Не включай это в деплое.

### HTTP resource limits

По умолчанию:

```text
MAX_REQUEST_BODY_BYTES=65536
MAX_REQUEST_HEADER_BYTES=65536
```

Для текущего API этого достаточно с большим запасом. Не увеличивай лимиты без конкретного endpoint, которому действительно нужен большой payload.

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

### Render

`render.yaml` создаёт **один Web Service** и запускает:

```text
python bot.py
```

Blueprint использует актуальное поле `runtime: python`. Render health check смотрит `/live`; проблемы MongoDB видны отдельно на `/ready` и не вызывают бессмысленный restart всего процесса.

> Free Render Web Service подходит для разработки/демо, но не для 24/7 бота: бесплатный instance может засыпать после периода без входящего HTTP/WebSocket-трафика. Для постоянно доступного бота используй always-on тариф/хостинг. Это ограничение платформы, а не задача `keep_alive`.

### Docker

```bash
docker build -t bible-bot .
docker run --env-file .env -p 8080:8080 bible-bot
```

## Структура

- `bot.py` — неизменённая legacy Telegram-логика: тесты, битвы, challenge, админка
- `intro.py` — явный compatibility import без дублирования данных
- `database.py` — MongoDB, статистика, bot sessions, achievements
- `questions/` — канонические данные вопросов
- `keep_alive.py` — загрузка `.env` и lifecycle Waitress внутри процесса бота
- `web_api/` — auth, HTTP routes, rate limiting, DB invariants и server-authoritative quiz API
- `miniapp/` — HTML/CSS/JS клиент
- `utils.py` — PNG/GIF результатов
- `tests/` — API/auth/session/hardening regression tests
- `.github/workflows/ci.yml` — compile + pytest + JS syntax check; actions pinned by full SHA
- `.github/dependabot.yml` — контролируемые dependency updates
- `docs/RESEARCH_WAVE2.md` — решения Wave 2 и 40+ первичных источников

## Важный принцип данных

`questions/` — единственный источник истины для вопросов. Дублирующий `miniapp/demo_questions.json` удалён: он быстро расходился бы с основной базой и позволял клиенту видеть правильные ответы. Если backend недоступен, Mini App показывает ошибку и не подделывает «офлайн-результат».

## Обновление зависимостей

Dependabot еженедельно проверяет pip и GitHub Actions. Minor/patch Python updates группируются. Major upgrade `python-telegram-bot` намеренно не автоматизирован: переход с 20.7 на текущую major-ветку должен быть отдельной миграционной волной с расширенными compatibility-тестами stateful bot runtime.

## Проверка перед релизом

```bash
python -m compileall -q .
pytest -q
node --check miniapp/app.js
```

CI запускает те же проверки на push и pull request. Полный research trail текущей волны — в `docs/RESEARCH_WAVE2.md`.
