# Аудит базы вопросов (1 Петра)

Отчёт сгенерирован `scripts/audit_question_quality.py`. Документ измеряет то, что не покрывают структурные тесты: глубину, верификацию, богословскую осторожность, читаемость и устойчивость к угадыванию.

## Сводка

- карточек в производственных пулах: **759**
- `blocker`: **0**
- `major`: **6**
- `minor`: **0**
- `info`: **68**
- вне пятой главы: `blocker` 0, `major` 0, `minor` 0, `info` 62
- пятая глава: reviewed bank закреплён blob-пинами; любое будущее изменение содержимого проходит только через новый reviewed release repin.
- покрытие послания: **105/105** стихов (1: 25/25, 2: 25/25, 3: 22/22, 4: 19/19, 5: 14/14)

Бюджет качества соблюдён: ни один счётчик не вырос.

## Метрики по пулам

| пул | карточек | ср. длина варианта | ср. длина объяснения | верный = самый длинный | сильная утечка | уклон позиции | якоря стихов | база/ядро/продвинутый |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `easy_p1` | 25 | 53 | 206 | 0% | 0% | 44% | 13 | 22/0/3 |
| `easy_p2` | 25 | 48 | 196 | 0% | 0% | 48% | 12 | 25/0/0 |
| `medium_p1` | 25 | 65 | 179 | 0% | 0% | 40% | 17 | 20/1/4 |
| `medium_p2` | 25 | 77 | 161 | 0% | 0% | 48% | 17 | 24/0/1 |
| `hard_p1` | 35 | 83 | 210 | 0% | 0% | 26% | 24 | 28/3/4 |
| `hard_p2` | 35 | 92 | 218 | 0% | 0% | 37% | 20 | 27/0/8 |
| `practical_p1` | 47 | 80 | 181 | 0% | 0% | 47% | 24 | 0/0/47 |
| `practical_p2` | 43 | 84 | 188 | 0% | 0% | 26% | 16 | 0/0/43 |
| `linguistics_ch1` | 15 | 66 | 170 | 0% | 0% | 47% | 10 | 0/0/15 |
| `linguistics_ch1_2` | 15 | 72 | 172 | 7% | 0% | 27% | 14 | 1/0/14 |
| `linguistics_ch1_3` | 15 | 62 | 196 | 0% | 0% | 47% | 11 | 0/0/15 |
| `nero` | 15 | 72 | 261 | 0% | 0% | 27% | 3 | 1/0/14 |
| `geography` | 10 | 43 | 218 | 10% | 0% | 30% | 3 | 3/0/7 |
| `tms_deep` | 15 | 107 | 202 | 0% | 0% | 27% | 14 | 4/11/0 |
| `intro1` | 15 | 92 | 174 | 0% | 0% | 27% | 4 | 0/0/15 |
| `intro2` | 16 | 95 | 154 | 0% | 0% | 25% | 5 | 1/0/15 |
| `intro3` | 16 | 92 | 153 | 0% | 0% | 25% | 12 | 0/0/16 |
| `chapter2` | 78 | 68 | 192 | 1% | 0% | 26% | 40 | 35/19/24 |
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

### `levels.derived_tiers_only` — info, 21

Пулы: `chapter2` (1), `chapter3` (1), `chapter4` (1), `chapter5` (1), `easy_p1` (1), `easy_p2` (1), `geography` (1), `hard_p1` (1), `hard_p2` (1), `intro1` (1), `intro2` (1), `intro3` (1), `linguistics_ch1` (1), `linguistics_ch1_2` (1), `linguistics_ch1_3` (1), `medium_p1` (1), `medium_p2` (1), `nero` (1), `practical_p1` (1), `practical_p2` (1), `tms_deep` (1)

- `chapter2` / `-`: difficulty mix is derived from reviewed metadata (base/core/advanced = 35/19/24); no card carries a reviewed level field
- `chapter3` / `-`: difficulty mix is derived from reviewed metadata (base/core/advanced = 44/50/71); no card carries a reviewed level field
- `chapter4` / `-`: difficulty mix is derived from reviewed metadata (base/core/advanced = 20/15/17); no card carries a reviewed level field
- `chapter5` / `-`: difficulty mix is derived from reviewed metadata (base/core/advanced = 28/21/23); no card carries a reviewed level field
- `easy_p1` / `-`: difficulty mix is derived from reviewed metadata (base/core/advanced = 22/0/3); this pool's name asserts base and the derived tier agrees for 22/25 cards, so the ladder is authored by pool, not per card
- … ещё 16

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
- `levels.*` — распределение по трудности. Лестница глав 1 задана именами пулов: `easy_p1` 22/25 (base), `easy_p2` 25/25 (base), `hard_p1` 4/35 (advanced), `hard_p2` 8/35 (advanced), `medium_p1` 1/25 (core), `medium_p2` 0/25 (core). Уровень пула несут 170 карточек (base 50, core 50, advanced 70); остальные 589 получают производный уровень от `claim_type`/`confidence`, который в `medium_*`/`hard_*` совпадает с именем пула редко: там в основном `claim_type=text`, а схема относит его к `base`. Проверенного уровня у отдельной карточки вне главы 1 в банке нет. Развести курсы по уровням можно будет после того, как этот уровень появится у карточки.

## Что делать по приоритету

1. Вне пятой главы находок уровня `blocker`/`major`/`minor` нет. В пятой главе остаётся только явно учтённый non-info долг: `blocker` 0, `major` 6, `minor` 0.
   - `metadata.source_quorum`: 6
   - Эти source-quorum находки принадлежат Research-authority boundary: product-репозиторий не добавляет недостающие evidence edges самовольно; для их закрытия нужен новый reviewed Research release и последующий repin.
2. Добавить проверенное поле `level` (база/ядро/продвинутый) и развести курсы по уровням: `levels.derived_tiers_only` сейчас отмечает 21 пулов, где трудность выводится из `claim_type`/`confidence`, а не из отдельного проверенного поля.
3. INFO-находки — измеряемый контекст, а не скрытая веточная работа: `depth.recall_only` 12, `levels.derived_tiers_only` 21, `wiseness.correct_longest` 4, `wiseness.option_shape_spread` 31. `language.latin_jargon` по пулам: 0. Базовые recall-карточки, различия длины в коротких label-наборах и производные уровни остаются видимыми в ratchet и не объявляются исправленными только потому, что они не блокируют релиз.
4. Каждая новая карточка проходит `--check`: ratchet в `data/question-quality-budget.json` не даёт счётчикам вырасти, а `tests/test_question_depth_regressions.py` держит глубину объяснений, нейтральность длины вариантов и источник-quorum вне пятой главы.