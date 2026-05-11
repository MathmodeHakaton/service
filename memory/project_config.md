---
name: project_config
description: Конфигурация проекта RU Liquidity Sentinel — архитектура, источники данных, правила модулей
type: project
---

# RU Liquidity Sentinel — конфигурация проекта

## Суть проекта
Система раннего предупреждения стресса ликвидности рублёвого денежного рынка для ПСБ (Промсвязьбанк). VI Весенняя школа МАИ / Финансовый университет, май 2026.

Выход: LSI (Liquidity Stress Index) 0–100. Пороги: 🟢 0–40 норма / 🟡 40–70 внимание / 🔴 70–100 стресс.

## Технологии
- Python 3.10+, Streamlit, Plotly, Pandas, NumPy, scipy
- PostgreSQL + SQLAlchemy (не активно используется)
- APScheduler (планировщик, TODO: persist results)
- Ollama (LLM, бонусный модуль)

## Структура
```
src/
  domain/modules/       — M1–M5 (бизнес-логика)
  domain/normalization/ — MAD нормализация
  domain/aggregation/   — LSI engine
  infrastructure/fetchers/ — ЦБ, Минфин, ФНС, Росказна
  application/pipeline.py  — оркестратор
  presentation/pages/   — Streamlit страницы
cache/cbr/             — кешированные данные ЦБ
pars_data/             — спарсенные данные (CSV)
```

## Активные страницы Streamlit
- `app.py` — главная (LSI, карточки модулей, без графиков)
- `1_M1.py` — резервы + RUONIA (динамический период, без оценки)
- `2_M2.py` — репо ЦБ (динамический период + флаги, без оценки)
- `3_M3.py` — ОФЗ Минфина (динамический период, без оценки)
- `4_M4.py` — налоговый календарь (С оценкой, без динамики)
- `5_M5.py` — казначейство (динамический период, профицит сверху, без оценки)
- `6_LSI.py` — заглушка "в разработке"
- `7_LLM.py` — LLM чат

## Источники данных

### ЦБ РФ (CBRFetcher)
- Резервы Excel: https://www.cbr.ru/vfs/hd_base/RReserves/required_reserves_table.xlsx
- RUONIA: https://www.cbr.ru/hd_base/ruonia/dynamics/
- Репо все сроки: https://www.cbr.ru/hd_base/repo/?UniDbQuery.P1=0&from=21.11.2002 ← P1=0 важно
- Параметры репо: https://www.cbr.ru/hd_base/dirrepoauctionparam/
- Ключевая ставка: https://www.cbr.ru/hd_base/keyrate/
- **bliquidity (15 колонок)**: https://www.cbr.ru/hd_base/bliquidity/?from=01.02.2014 ← SSL обход нужен

### bliquidity — ключевые колонки по ТЗ
- col2 `structural_balance_bln` — дефицит/профицит ⭐⭐ ground truth LSI
  - (+) = дефицит (банки занимают у ЦБ) = стресс
  - (−) = профицит (банки размещают в ЦБ) = норма
- col5 `auction_repo_bln` — аукционное репо ⭐ M2
- col7 `standing_repo_bln` — репо пост. действия (стресс-сигнал M2)
- col8 `standing_secured_credit_bln` — обесп. кредиты пост. действия ⭐ M2
- col10 `auction_deposits_bln` — аукц. депозиты (профицит-сигнал M5)
- col11 `standing_deposits_bln` — депозиты пост. действия (профицит M5)
- col14 `corr_accounts_bln` — корсчета ⭐⭐ M1 факт, M5 бюджетный канал
- col15 `required_reserves_bln` — норматив резервов ⭐⭐ M1

### Минфин (MinfinFetcher)
- ОФЗ аукционы текущий год: https://minfin.gov.ru/ru/document/?id_4=315131
- ОФЗ история 2016–2026 (Excel): https://minfin.gov.ru/ru/perfomance/public_debt/internal/operations/ofz/auction
  - "Таблицы по результатам" → страницы 1 и 2 → 11 Excel файлов
  - Парсер: `parse_m3_ofz.py` → `pars_data/m3_ofz_full.csv` (866 аукционов)

### Росказна
- SSL недоступен через стандартный Python, но `requests verify=False` работает
- Данные ЕКС-депозитов: XML файлы, только последние 10 дней, истории нет
- Парсер Росказны: `parse_m3_ofz_docs.py` (не запускался до конца)
- `pars_data/m5_roskazna.csv` — 36 аукционов апрель-май 2026

### ФНС
- Сайт JS, не парсится. Генерируем программно по НК РФ (FNSFetcher)

## Модули и их сигналы

### M1 — Резервы + RUONIA
**Источник**: bliquidity col14+col15 (приоритет) или Excel резервов (fallback)
**Сигналы**: MAD_score_спред (col14−col15), MAD_score_RUONIA, Flag_EndOfPeriod, Flag_AboveKey
**Спред M1** = corr_accounts_bln − required_reserves_bln (col14 − col15)
**Веса LSI**: 38.7% (SNR=3.62)

### M2 — Репо ЦБ
**Источник приоритет**: `cache/cbr/repo_full.csv` (358 аукционов, со спросом, cover_ratio)
**Источник fallback**: `cache/cbr/repo_results.csv` (6150 строк, с 2002, все сроки, без спроса)
**Сигналы TZ**: MAD_score_cover (cover=спрос/размещение), MAD_score_rate_spread, Flag_Demand (cover>2.0)
**Новые сигналы из bliquidity**: MAD_score_auction_repo (col5), total_emergency_bln (col7+col8), Flag_Emergency
**Важно**: ЦБ публикует только объём размещения = лимит, спрос недоступен в HTML-таблице репо
**Веса LSI**: 37.4% (SNR=3.50)

### M3 — ОФЗ Минфин
**Источник**: `cache/minfin/ofz_auctions.csv` (866 аукционов 2016–2026)
**cover_ratio** = спрос/размещение (отклонение от ТЗ: ТЗ говорит спрос/предложение, но размещение честнее)
**Сигналы**: MAD_score_cover, MAD_score_yield_spread, Flag_Nedospros (<1.2), Flag_Perespros (>2.0)
**Веса LSI**: 15.2% (SNR=1.42)

### M4 — Налоговый календарь
**Источник**: генерируется программно по НК РФ (FNSFetcher)
**Роль**: мультипликатор SF (1.0–1.4), не аддитивный
**SF**: 1.4 конец квартала / 1.2 конец месяца / 1.1 налоговая неделя / 1.0 норма
**Веса LSI**: мультипликатор

### M5 — Казначейство
**Источник**: bliquidity (все 15 колонок)
**MAD_score_ЦБ**: MAD(col2 structural_balance, окно 260д)
**MAD_score_Росказна**: MAD(недельная дельта col14 corr_accounts, окно 156 нед)
**Flag_Budget_Drain**: пики оттока >500 млрд/нед, distance=8 нед → 22 события за 7 лет
**Новые**: MAD_score_депозиты (col10+col11), Flag_Proficit (депозиты>1000 млрд)
**Конвенция**: balance>0 = дефицит = стресс; balance<0 = профицит = норма
**График**: инвертирован — профицит сверху (+), дефицит снизу (−)
**Веса LSI**: 8.8% (SNR=0.82)

## LSI агрегация
```
base_LSI = M1×0.387 + M2×0.374 + M3×0.152 + M5×0.088
final_LSI = sigmoid(base_LSI) × Seasonal_Factor_M4
LSI_100 = clip(final_LSI × 100, 0, 100)
```
**Веса по SNR**: M1=3.62, M2=3.50, M3=1.42, M5=0.82 на эпизодах дек.2014, фев.2022, авг.2023

## Ключевые файлы
- `cache/cbr/bliquidity.csv` — 15 колонок, 3077 строк, 2014–2026
- `cache/cbr/repo_full.csv` — 358 аукционов со спросом и cover_ratio
- `cache/cbr/repo_results.csv` — 6150 строк все сроки с 2002
- `cache/minfin/ofz_auctions.csv` — 866 аукционов ОФЗ 2016–2026
- `pars_data/m3_ofz_full.csv` — то же в русских колонках (utf-8-sig)
- `pars_data/m2_repo_cbr.csv` — репо в русских колонках
- `parse_m3_ofz.py` — парсер ОФЗ Минфина (Excel, все годы)
- `nujn.md` — обоснование выбора сигналов для чекпоинта

## Правила UI
- Оценка (st.metric "Стресс-оценка") только на M4, на остальных убрана
- Динамический выбор периода (кнопки + date_input) на M1, M2, M3, M5
- M2: флаги пересчитываются внутри выбранного периода (вариант Б)
- M5: профицит сверху (значения инвертированы)
- MAD-сигналы график убран из M1 (только верхний)
- Нижний график доходности убран из M3

## Известные расхождения с ТЗ
1. LSI внутренне 0–1, умножается на 100 для отображения — ОК
2. M2 Flag_Demand: ТЗ cover>2.0, реализовано через MAD>2σ как прокси (нет данных спроса в HTML репо)
3. M3 cover: используем спрос/размещение вместо спрос/предложение (обоснованное отклонение)
4. M4: генерация по НК РФ вместо парсинга ФНС (JS сайт)
5. M5 Росказна: используем bliquidity как proxy (SSL недоступен)
6. Backtest на эпизодах + sensitivity analysis ±20% — не реализованы
7. LSI страница (6_LSI.py) — заглушка

## .claude/settings.local.json
Разрешены: WebFetch(domain:www.nalog.gov.ru), WebFetch(domain:minfin.gov.ru)
