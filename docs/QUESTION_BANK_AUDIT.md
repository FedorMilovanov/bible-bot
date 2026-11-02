# Аудит базы вопросов (1 Петра)

Отчёт сгенерирован `scripts/audit_question_quality.py`. Документ измеряет то, что не покрывают структурные тесты: глубину, верификацию, богословскую осторожность, читаемость и устойчивость к угадыванию.

## Сводка

- карточек в производственных пулах: **759**
- `blocker`: **0**
- `major`: **6**
- `minor`: **0**
- `info`: **50**
- вне пятой главы: `blocker` 0, `major` 0, `minor` 0, `info` 44
- пятая глава: reviewed bank закреплён blob-пинами; любое будущее изменение содержимого проходит только через новый reviewed release repin.
- покрытие послания: **105/105** стихов (1: 25/25, 2: 25/25, 3: 22/22, 4: 19/19, 5: 14/14)

Бюджет качества соблюдён: ни один счётчик не вырос.

## Метрики по пулам

| пул | карточек | ср. длина варианта | ср. длина объяснения | верный = самый длинный | сильная утечка | уклон позиции | якоря стихов | база/ядро/продвинутый |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `easy_p1` | 25 | 53 | 206 | 0% | 0% | 44% | 13 | 25/0/0 |
| `easy_p2` | 25 | 48 | 196 | 0% | 0% | 48% | 12 | 25/0/0 |
| `medium_p1` | 25 | 65 | 179 | 0% | 0% | 40% | 17 | 8/14/3 |
| `medium_p2` | 25 | 77 | 161 | 0% | 0% | 48% | 17 | 4/19/2 |
| `hard_p1` | 35 | 88 | 232 | 0% | 0% | 26% | 24 | 3/17/15 |
| `hard_p2` | 35 | 96 | 230 | 0% | 0% | 37% | 20 | 0/15/20 |
| `practical_p1` | 47 | 80 | 181 | 0% | 0% | 47% | 24 | 1/39/7 |
| `practical_p2` | 43 | 84 | 188 | 0% | 0% | 26% | 16 | 0/35/8 |
| `linguistics_ch1` | 15 | 66 | 170 | 0% | 0% | 47% | 10 | 3/8/4 |
| `linguistics_ch1_2` | 15 | 72 | 172 | 7% | 0% | 27% | 14 | 3/7/5 |
| `linguistics_ch1_3` | 15 | 62 | 196 | 0% | 0% | 47% | 11 | 2/8/5 |
| `nero` | 15 | 72 | 261 | 0% | 0% | 27% | 3 | 12/3/0 |
| `geography` | 10 | 43 | 218 | 10% | 0% | 30% | 3 | 9/1/0 |
| `tms_deep` | 15 | 119 | 250 | 0% | 0% | 27% | 14 | 3/7/5 |
| `intro1` | 15 | 92 | 174 | 0% | 0% | 27% | 4 | 9/5/1 |
| `intro2` | 16 | 100 | 183 | 0% | 0% | 25% | 5 | 3/7/6 |
| `intro3` | 16 | 105 | 247 | 0% | 0% | 25% | 13 | 3/7/6 |
| `chapter2` | 78 | 68 | 192 | 1% | 0% | 26% | 40 | 52/11/15 |
| `chapter3` | 165 | 84 | 186 | 0% | 0% | 26% | 56 | 44/50/71 |
| `chapter4` | 52 | 87 | 187 | 0% | 0% | 25% | 24 | 20/15/17 |
| `chapter5` | 72 | 110 | 204 | 1% | 0% | 25% | 22 | 28/21/23 |

## Проверки

### `depth.recall_only` — info, 12

Пулы: `chapter2` (3), `chapter5` (3), `geography` (2), `chapter3` (1), `chapter4` (1), `easy_p1` (1), `easy_p2` (1)

- `chapter2` / `ch2_text_001`: single-clause recall item
- `chapter2` / `ch2_text_010`: single-clause recall item
- `chapter2` / `ch2_text_018`: single-clause recall item
- `chapter3` / `ch3_text_302`: single-clause recall item
- `chapter4` / `ch4_text_005`: single-clause recall item
- … ещё 7

### `levels.derived_tiers_only` — info, 3

Пулы: `chapter3` (1), `chapter4` (1), `chapter5` (1)

- `chapter3` / `-`: difficulty mix still uses metadata proxy for 165/165 cards (base/core/advanced = 44/50/71)
- `chapter4` / `-`: difficulty mix still uses metadata proxy for 52/52 cards (base/core/advanced = 20/15/17)
- `chapter5` / `-`: difficulty mix still uses metadata proxy for 72/72 cards (base/core/advanced = 28/21/23)

### `metadata.source_quorum` — major, 6

Пулы: `chapter5` (6)

- `chapter5` / `ch5_w3q_054`: application/project has 1 source(s), policy quorum 2
- `chapter5` / `ch5_w3q_063`: application/project has 1 source(s), policy quorum 2
- `chapter5` / `ch5_w3q_065`: greek/neutral has 1 source(s), policy quorum 2
- `chapter5` / `ch5_w3q_080`: history/neutral has 1 source(s), policy quorum 2
- `chapter5` / `ch5_w3q_111`: application/project has 1 source(s), policy quorum 2
- … ещё 1

### `wiseness.correct_longest` — info, 4

Пулы: `chapter2` (1), `chapter5` (1), `geography` (1), `linguistics_ch1_2` (1)

- `chapter2` / `ch2_ot_005`: correct option is uniquely longest (11 vs max 9); label set - the difference is the length of a name, reference or grammatical tag
- `chapter5` / `ch5_w3q_056`: correct option is uniquely longest (36 vs max 31); label set - the difference is the length of a name, reference or grammatical tag
- `geography` / `geo_06`: correct option is uniquely longest (9 vs max 6); label set - the difference is the length of a name, reference or grammatical tag
- `linguistics_ch1_2` / `ling2_06`: correct option is uniquely longest (10 vs max 8); label set - the difference is the length of a name, reference or grammatical tag

### `wiseness.option_shape_spread` — info, 31

Пулы: `chapter2` (7), `easy_p1` (5), `easy_p2` (4), `geography` (3), `medium_p1` (3), `linguistics_ch1_3` (2), `chapter4` (1), `chapter5` (1), `hard_p1` (1), `hard_p2` (1), `linguistics_ch1` (1), `nero` (1), `practical_p1` (1)

- `chapter2` / `ch2_gr_006`: options are not comparable in shape (19..47); label set - lexical length only
- `chapter2` / `ch2_ot_006`: options are not comparable in shape (4..10); label set - lexical length only
- `chapter2` / `ch2_text_003`: options are not comparable in shape (16..45); label set - lexical length only
- `chapter2` / `ch2_text_007`: options are not comparable in shape (12..29); label set - lexical length only
- `chapter2` / `ch2_text_013`: options are not comparable in shape (9..41); label set - lexical length only
- … ещё 26

## Блокеры (`blocker`)

Блокеров нет.

## Верификация цитат

- 1 Пет: **1218** строк MorphGNT/SBLGNT (из 1678), офлайн-проверка нашла **2**.
  - источник: https://github.com/morphgnt/sblgnt/blob/master/81-1Pe-morphgnt.txt
  - `greek.form_in_stem_not_in_corpus` `chapter5` / `ch5_w3q_075`: ἑστήκατε is not a 1 Peter form or lemma in the evidence excerpt
  - `greek.form_in_stem_not_in_corpus` `chapter5` / `ch5_w3q_144`: ἐκκλησία is not a 1 Peter form or lemma in the evidence excerpt
- LXX/ВЗ: **72** карточек с ветхозаветными ссылками, **578** стихов Рафлса 1935; офлайн-проверка нашла **0**.
  - прошлый прогон по корпусу: информационные находки lxx.word_in_option_not_in_verses × 3; блокирующих нет.
  - корпус LXX не вендорится (CC BY-NC-SA/CCAT): хранится запись ссылок и хеш цитат, `data/ot-citation-fingerprints.json`.
- Разбор форм: **79** карточек-кандидатов с разбором на якоре 1 Петра; **56** действительно сверены с **1218** строками MorphGNT, **23** явно оставлены для ручной проверки; блокирующих расхождений — **0**.
  - ручная граница: несколько форм в одном вопросе или форма не названа; `scripts/verify_parse_claims.py`, тесты `tests/test_parse_claims.py` фиксируют и машинное покрытие, и полный список таких исключений.

## Как читать отчёт

- `wiseness.*` — можно ли угадать ответ без знания текста (длина, форма, позиция вариантов).
- `depth.*` — учит ли карточка чему-то за пределами одного пересказа стиха.
- `language.*` — поймёт ли обычный читатель формулировку; внутренний язык конвейера исследований (`inspected`, `HOLD`, `Wave3n`, `production-status`) — блокер.
- `disputed.*` — есть ли обязательное покрытие спорных мест и не выдаётся ли спор за факт.
- `content.trivia` — факт о древнем мире, который не нужен для чтения послания.
- `levels.*` — распределение по трудности. Индивидуально проверенный когнитивный уровень уже имеют 470 карточек (base 165, core 203, advanced 102). Остальные 289 пока получают только audit-прокси (base 92, core 86, advanced 111) из claim_type/confidence; этот прокси не считается продуктовым уровнем. Развести курсы по уровням можно будет после того, как этот уровень появится у карточки.

## Что делать по приоритету

1. Вне пятой главы находок уровня `blocker`/`major`/`minor` нет. В пятой главе остаётся только явно учтённый non-info долг: `blocker` 0, `major` 6, `minor` 0.
   - `metadata.source_quorum`: 6
   - Эти source-quorum находки принадлежат Research-authority boundary: product-репозиторий не добавляет недостающие evidence edges самовольно; для их закрытия нужен новый reviewed Research release и последующий repin.
2. Добавить проверенное поле `level` (база/ядро/продвинутый) и развести курсы по уровням: `levels.derived_tiers_only` сейчас отмечает 3 пулов, где трудность выводится из `claim_type`/`confidence`, а не из отдельного проверенного поля.
3. INFO-находки — измеряемый контекст, а не скрытая веточная работа: `depth.recall_only` 12, `levels.derived_tiers_only` 3, `wiseness.correct_longest` 4, `wiseness.option_shape_spread` 31. `language.latin_jargon` по пулам: 0. Базовые recall-карточки, различия длины в коротких label-наборах и производные уровни остаются видимыми в ratchet и не объявляются исправленными только потому, что они не блокируют релиз.
4. Каждая новая карточка проходит `--check`: ratchet в `data/question-quality-budget.json` не даёт счётчикам вырасти, а `tests/test_question_depth_regressions.py` держит глубину объяснений, нейтральность длины вариантов и источник-quorum вне пятой главы.
