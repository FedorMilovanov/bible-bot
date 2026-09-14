# Аудит базы вопросов (1 Петра)

Отчёт сгенерирован `scripts/audit_question_quality.py`. Документ измеряет то, что не покрывают структурные тесты: глубину, верификацию, богословскую осторожность, читаемость и устойчивость к угадыванию.

## Сводка

- карточек в производственных пулах: **759**
- `blocker`: **6**
- `major`: **127**
- `minor`: **72**
- `info`: **72**
- вне пятой главы (её банки заперты блоб-пинами и ждут выпускного repin): `blocker` 0, `major` 0, `minor` 0, `info` 64
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
| `linguistics_ch1` | 15 | 66 | 161 | 0% | 0% | 47% | 10 | 0/0/15 |
| `linguistics_ch1_2` | 15 | 72 | 169 | 7% | 0% | 27% | 14 | 1/0/14 |
| `linguistics_ch1_3` | 15 | 62 | 180 | 0% | 0% | 47% | 11 | 0/0/15 |
| `nero` | 15 | 72 | 255 | 0% | 0% | 27% | 3 | 1/0/14 |
| `geography` | 10 | 43 | 215 | 10% | 0% | 30% | 3 | 3/0/7 |
| `tms_deep` | 15 | 107 | 202 | 0% | 0% | 27% | 14 | 4/11/0 |
| `intro1` | 15 | 92 | 174 | 0% | 0% | 27% | 4 | 0/0/15 |
| `intro2` | 16 | 95 | 154 | 0% | 0% | 25% | 5 | 1/0/15 |
| `intro3` | 16 | 92 | 153 | 0% | 0% | 25% | 12 | 0/0/16 |
| `chapter2` | 78 | 68 | 185 | 1% | 0% | 26% | 40 | 35/19/24 |
| `chapter3` | 165 | 77 | 191 | 0% | 0% | 26% | 55 | 44/50/71 |
| `chapter4` | 52 | 82 | 153 | 0% | 0% | 25% | 24 | 20/15/17 |
| `chapter5` | 72 | 60 | 98 | 83% | 75% | 25% | 22 | 28/21/23 |

## Проверки

### `depth.explanation_short` — minor, 23

Пулы: `chapter5` (23)

- `chapter5` / `ch5_w3q_048`: explanation is 87 chars (<90)
- `chapter5` / `ch5_w3q_052`: explanation is 72 chars (<90)
- `chapter5` / `ch5_w3q_053`: explanation is 86 chars (<90)
- `chapter5` / `ch5_w3q_055`: explanation is 73 chars (<90)
- `chapter5` / `ch5_w3q_056`: explanation is 77 chars (<90)
- … ещё 18

### `depth.explanation_very_short` — major, 3

Пулы: `chapter5` (3)

- `chapter5` / `ch5_w3q_070`: explanation is 56 chars (<60)
- `chapter5` / `ch5_w3q_090`: explanation is 52 chars (<60)
- `chapter5` / `ch5_w3q_127`: explanation is 51 chars (<60)

### `depth.meta_phrasing` — major, 4

Пулы: `chapter5` (4)

- `chapter5` / `ch5_w3q_068`: stem asks how to phrase the claim: 'Как корректно формулировать наблюдение о καταρτίσει, στηρίξει, σθενώσε'
- `chapter5` / `ch5_w3q_080`: stem asks how to phrase the claim: 'Как корректно представить основные исторические варианты местонахожден'
- `chapter5` / `ch5_w3q_106`: stem asks how to phrase the claim: '[Позиция курса] Как суммировать этические границы лидерства в 1 Пет. 5'
- `chapter5` / `ch5_w3q_111`: stem asks how to phrase the claim: '[Позиция курса] Как формулировать практический призыв 1 Пет. 5:12 с уч'

### `depth.recall_only` — info, 12

Пулы: `chapter2` (3), `chapter5` (3), `geography` (2), `chapter3` (1), `chapter4` (1), `easy_p1` (1), `easy_p2` (1)

- `chapter2` / `ch2_text_001`: single-clause recall item
- `chapter2` / `ch2_text_010`: single-clause recall item
- `chapter2` / `ch2_text_018`: single-clause recall item
- `chapter3` / `ch3_text_302`: single-clause recall item
- `chapter4` / `ch4_text_005`: single-clause recall item
- … ещё 7

### `language.pipeline_vocabulary_in_explanation` — major, 4

Пулы: `chapter5` (4)

- `chapter5` / `ch5_w3q_050`: hold-workflow in explanation
- `chapter5` / `ch5_w3q_051`: internal-english in explanation
- `chapter5` / `ch5_w3q_075`: artifact-vocabulary in explanation
- `chapter5` / `ch5_w3q_125`: internal-english in explanation

### `language.pipeline_vocabulary_in_stem` — blocker, 6

Пулы: `chapter5` (6)

- `chapter5` / `ch5_w3q_050`: internal-english visible to the learner
- `chapter5` / `ch5_w3q_075`: internal-english visible to the learner
- `chapter5` / `ch5_w3q_111`: artifact-vocabulary visible to the learner
- `chapter5` / `ch5_w3q_125`: artifact-vocabulary visible to the learner
- `chapter5` / `ch5_w3q_127`: internal-english visible to the learner
- … ещё 1

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

### `wiseness.correct_longest` — major, 63

Пулы: `chapter5` (60), `chapter2` (1), `geography` (1), `linguistics_ch1_2` (1)

- `chapter5` / `ch5_w3q_046`: correct option is uniquely longest (70 vs max 56)
- `chapter5` / `ch5_w3q_047`: correct option is uniquely longest (126 vs max 74)
- `chapter5` / `ch5_w3q_049`: correct option is uniquely longest (68 vs max 60)
- `chapter5` / `ch5_w3q_050`: correct option is uniquely longest (132 vs max 62)
- `chapter5` / `ch5_w3q_051`: correct option is uniquely longest (98 vs max 53)
- … ещё 58

### `wiseness.length_leak` — major, 54

Пулы: `chapter5` (54)

- `chapter5` / `ch5_w3q_047`: length alone reveals the answer (126 vs [39, 47, 74])
- `chapter5` / `ch5_w3q_050`: length alone reveals the answer (132 vs [50, 55, 62])
- `chapter5` / `ch5_w3q_051`: length alone reveals the answer (98 vs [50, 50, 53])
- `chapter5` / `ch5_w3q_052`: length alone reveals the answer (108 vs [66, 73, 84])
- `chapter5` / `ch5_w3q_053`: length alone reveals the answer (124 vs [42, 64, 72])
- … ещё 49

### `wiseness.option_shape_spread` — minor, 78

Пулы: `chapter5` (46), `chapter2` (7), `easy_p1` (5), `easy_p2` (4), `geography` (3), `medium_p1` (3), `chapter3` (2), `linguistics_ch1_3` (2), `chapter4` (1), `hard_p1` (1), `hard_p2` (1), `linguistics_ch1` (1), `nero` (1), `practical_p1` (1)

- `chapter5` / `ch5_w3q_047`: options are not comparable in shape (39..126)
- `chapter5` / `ch5_w3q_050`: options are not comparable in shape (50..132)
- `chapter5` / `ch5_w3q_053`: options are not comparable in shape (42..124)
- `chapter5` / `ch5_w3q_054`: options are not comparable in shape (33..92)
- `chapter5` / `ch5_w3q_057`: options are not comparable in shape (37..94)
- … ещё 73

### `wiseness.trivial_distractor` — minor, 3

Пулы: `chapter5` (3)

- `chapter5` / `ch5_w3q_053`: unfalsifiable/joke distractor: 'Текст вообще не говорит о лидерстве общины'
- `chapter5` / `ch5_w3q_058`: unfalsifiable/joke distractor: '1 Петра вообще не использует формулу о гордых и смиренных'
- `chapter5` / `ch5_w3q_107`: unfalsifiable/joke distractor: 'Потому что в стихе вообще нет обращения к младшим'

## Блокеры (`blocker`)

- `language.pipeline_vocabulary_in_stem` `chapter5` / `ch5_w3q_050` (принятый долг): internal-english visible to the learner
- `language.pipeline_vocabulary_in_stem` `chapter5` / `ch5_w3q_075` (принятый долг): internal-english visible to the learner
- `language.pipeline_vocabulary_in_stem` `chapter5` / `ch5_w3q_111` (принятый долг): artifact-vocabulary visible to the learner
- `language.pipeline_vocabulary_in_stem` `chapter5` / `ch5_w3q_125` (принятый долг): artifact-vocabulary visible to the learner
- `language.pipeline_vocabulary_in_stem` `chapter5` / `ch5_w3q_127` (принятый долг): internal-english visible to the learner
- `language.pipeline_vocabulary_in_stem` `chapter5` / `ch5_w3q_143` (принятый долг): artifact-vocabulary visible to the learner

### Принятый долг (требует выпускного repin)

Эти блокеры перечислены в `data/question-quality-budget.json` → `accepted_blockers` с причиной. Список закреплён тестом `tests/test_question_bank_audit.py`, поэтому он не может вырасти без отдельного ревью.

## Верификация цитат

- 1 Пет: **1218** строк MorphGNT/SBLGNT (из 1678), офлайн-проверка нашла **2**.
  - источник: https://github.com/morphgnt/sblgnt/blob/master/81-1Pe-morphgnt.txt
  - `greek.form_in_stem_not_in_corpus` `chapter5` / `ch5_w3q_075`: ἑστήκατε is not a 1 Peter form or lemma in the evidence excerpt
  - `greek.form_in_stem_not_in_corpus` `chapter5` / `ch5_w3q_144`: ἐκκλησία is not a 1 Peter form or lemma in the evidence excerpt
- LXX/ВЗ: **72** карточек с ветхозаветными ссылками, **578** стихов Рафлса 1935; офлайн-проверка нашла **0**.
  - прошлый прогон по корпусу: информационные находки lxx.word_in_option_not_in_verses × 3; блокирующих нет.
  - корпус LXX не вендорится (CC BY-NC-SA/CCAT): хранится запись ссылок и хеш цитат, `data/ot-citation-fingerprints.json`.

## Как читать отчёт

- `wiseness.*` — можно ли угадать ответ без знания текста (длина, форма, позиция вариантов).
- `depth.*` — учит ли карточка чему-то за пределами одного пересказа стиха.
- `language.*` — поймёт ли обычный читатель формулировку; внутренний язык конвейера исследований (`inspected`, `HOLD`, `Wave3n`, `production-status`) — блокер.
- `disputed.*` — есть ли обязательное покрытие спорных мест и не выдаётся ли спор за факт.
- `content.trivia` — факт о древнем мире, который не нужен для чтения послания.
- `levels.*` — распределение по трудности. Лестница глав 1 задана именами пулов: `easy_p1` 22/25 (base), `easy_p2` 25/25 (base), `hard_p1` 4/35 (advanced), `hard_p2` 8/35 (advanced), `medium_p1` 1/25 (core), `medium_p2` 0/25 (core). Уровень пула несут 170 карточек (base 50, core 50, advanced 70); остальные 589 получают производный уровень от `claim_type`/`confidence`, который в `medium_*`/`hard_*` совпадает с именем пула редко: там в основном `claim_type=text`, а схема относит его к `base`. Проверенного уровня у отдельной карточки вне главы 1 в банке нет. Развести курсы по уровням можно будет после того, как этот уровень появится у карточки.

## Что делать по приоритету

1. Вне пятой главы находок уровня `blocker`/`major`/`minor` нет: единственный оставшийся долг — пятая глава (6 блокеров в `accepted_blockers`, всего 213 находок). Её банки заперты блоб-пинами, поэтому содержимое меняет только выпускной repin; правка на месте обошла бы выпускное ревью (`docs/CHAPTER5_RELEASE_AUDIT.md`).
2. Добавить проверенное поле `level` (база/ядро/продвинутый) и развести курсы по уровням: `levels.derived_tiers_only` сейчас отмечает 21 пулов, где трудность выводится из `claim_type`/`confidence`, а не из отдельного проверенного поля.
3. Все прочие находки — INFO-контекст, а не дефекты (`depth.recall_only` 12, `levels.derived_tiers_only` 21, `wiseness.correct_longest` 5, `wiseness.option_shape_spread` 34): базовые recall-карточки для простых пользователей, метки/ссылки в вариантах, для которых выравнивание длины меняло бы сам проверяемый факт, и производные уровни.
4. Каждая новая карточка проходит `--check`: ratchet в `data/question-quality-budget.json` не даёт счётчикам вырасти, а `tests/test_question_depth_regressions.py` держит глубину объяснений, нейтральность длины вариантов и источник-quorum вне пятой главы.
