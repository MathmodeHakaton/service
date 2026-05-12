# RU Liquidity Sentinel

Система раннего предупреждения стресса ликвидности рублёвого денежного рынка.
Собирает данные ЦБ РФ, Минфина, ФНС и Росказны, считает Liquidity Stress Index
(LSI, 0–100) на CatBoost-агрегаторе пяти модулей, объясняет результат через
SHAP и аналитический комментарий LLM. RAG-чат отвечает по выгруженным данным
системы.

**Развёрнутая версия:** https://service-rjtqfztbsmuf8rzkzpjklv.streamlit.app

---

## Как работает

```
парсеры (CBR / Minfin / FNS / Roskazna) → БД-кэш
                ↓
       ml_model/data/*.csv  (история M1..M5)
                ↓
  feature engineering + MAD-нормализация (1-year rolling, past-only)
                ↓
       stress components per module (z → 0..100)
                ↓
   CatBoost regressor (weekly retrain / daily inference)
                ↓
      × seasonal_factor (M4 как мультипликатор)
                ↓
   SHAP-разложение → contribution_M1..M5
                ↓
   Kalman 1D smoothing + hysteresis status (green/yellow/red)
                ↓
       data/model_artifacts/lsi_*.csv
                ↓
  ┌────────────────────────────────────────────┐
  │ Streamlit-дашборд (страницы M1..M5, LSI)   │
  │ Yandex GPT — авто-комментарий              │
  │ RAG-чат: knowledge base из артефактов      │
  │          + сырые сигналы по годам          │
  └────────────────────────────────────────────┘
```

Ключевые компоненты:

- **5 модулей** (M1..M5) собирают сигналы по своим источникам данных:
  M1 — корсчета банков + RUONIA, M2 — аукционы репо ЦБ, M3 — аукционы ОФЗ
  Минфина, M4 — налоговая сезонность (применяется как мультипликатор),
  M5 — структурный баланс ликвидности и средства казначейства.
- **Агрегатор LSI** — CatBoost, обученный на weak-target из CBR bliquidity.
  Веса модулей не задаются вручную, а LEARNED моделью; разные дни — разные
  веса. SHAP даёт per-day разложение прогноза на вклад каждого модуля.
- **Daily inference** (5–20 сек) и **weekly retrain** (1–3 мин) разнесены:
  ежедневно модель только предсказывает, раз в неделю переобучается.
- **LLM-комментарий** на странице LSI и **RAG-чат** на отдельной странице —
  через Yandex AI Studio по OpenAI-совместимому endpoint. Чат отвечает строго
  по выгруженным документам системы; off-topic блокируется локальным
  guardrail'ом плюс жёстким system-промптом.

Подробный пайплайн обоих частей: [docs/LSI_INTEGRATION_REPORT.md](docs/LSI_INTEGRATION_REPORT.md).
Метрики качества: [docs/METRICS_REPORT.md](docs/METRICS_REPORT.md).

---

## Структура проекта

```
service/
├── config/                       # настройки приложения (env-driven)
│   ├── settings.py               # Pydantic Settings: БД, ключи Yandex, TTL
│   └── constants.py
│
├── src/
│   ├── application/              # Use-cases / оркестрация
│   │   ├── pipeline.py           # Pipeline.execute_full(): фетчеры + сигналы + LSI
│   │   ├── lsi_refresh.py        # mode=inference|retrain → ml_model → артефакты
│   │   ├── scheduler.py          # APScheduler: daily inference, weekly retrain
│   │   └── backtest.py
│   │
│   ├── domain/                   # Чистые бизнес-сущности
│   │   ├── modules/              # M1..M5 — расчёт сигналов модулей
│   │   ├── aggregation/          # LSIEngine — обёртка над агрегатором
│   │   ├── models/               # Pydantic-модели LSI / Snapshot
│   │   ├── normalization/        # MAD z-score
│   │   └── llm/                  # prompt-builder (статика)
│   │
│   ├── infrastructure/           # Внешние интеграции
│   │   ├── fetchers/             # CBR / Minfin / FNS / Roskazna парсеры
│   │   └── storage/              # БД-кэш (Postgres + SQLAlchemy), миграции
│   │
│   └── presentation/             # Streamlit-фронт
│       ├── app.py                # точка входа дашборда
│       ├── data_loader.py
│       ├── components/           # gauge, charts, module cards
│       ├── pages/                # 1_M1, 2_M2, …, 6_LSI, 7_LLM, 8_Metrics?
│       └── rag/                  # RAG-агент
│           ├── knowledge_base.py # сбор чанков из артефактов + сырых сигналов
│           ├── retriever.py      # token-overlap + IDF + tag-boost (+ MMR)
│           ├── chat_llm.py       # system-промпт + format_context
│           ├── yandex_client.py  # openai-клиент к Yandex AI Studio
│           ├── guardrails.py     # prompt-injection + history filtering
│           ├── commentary_prompt.py
│           ├── query_rewrite.py  # LLM-переписчик коротких follow-up'ов
│           └── golden_set.py     # эталонные запросы для retriever-метрик
│
├── ml_model/                     # Отдельный проект ML-крышки
│   ├── data/                     # снэпшоты входных CSV (M1..M5)
│   ├── src/                      # features / stress_components / lsi_ml
│   ├── run_pipeline.py           # полный retrain
│   ├── inference.py              # predict + SHAP без fit
│   └── outputs/                  # генерируется при запуске
│
├── data/
│   ├── model_artifacts/          # артефакты для дашборда (читаются Streamlit'ом)
│   │   ├── lsi_dashboard_extract.csv     # срез для UI (свежий день + статус)
│   │   ├── lsi_timeseries.csv            # полная история со всеми фичами
│   │   ├── backtest_crisis_episodes.csv
│   │   ├── feature_importance.csv
│   │   ├── module_importance_catboost.csv
│   │   ├── lsi_ml_model.cbm              # сериализованная CatBoost-модель
│   │   └── lsi_ml_metadata.joblib        # feature_columns + metrics
│   └── ...                       # кэш Excel-источников ЦБ
│
├── migrations/                   # Yoyo миграции БД
├── scripts/                      # сервисные скрипты
│   ├── compute_metrics.py        # счёт + запись METRICS_REPORT.md / json
│   ├── clear_cache.py
│   ├── db_migrate.py
│   ├── health_check.py
│   └── cloud_init.py
│
├── tests/                        # pytest
├── docs/                         # отчёты для жюри (интеграция, метрики)
├── memory/
├── pars_data/                    # старые снэпшоты парсинга
│
├── main.py / run.py              # CLI-точки входа
├── parse_all.py / test_all.py    # one-off скрипты
│
├── Dockerfile / Dockerfile.streamlit / docker-compose.yml
├── pyproject.toml / requirements.txt / uv.lock
├── yoyo.ini
└── .env.example
```

---

## Запуск локально

### Зависимости
```bash
pip install -r requirements.txt
```

### Переменные окружения
Скопируй `.env.example` в `.env` и заполни:
```
DATABASE_URL=postgresql://...
YANDEX_API_KEY=...
YANDEX_FOLDER_ID=...
YANDEX_MODEL_COMMENTARY=yandexgpt-5-lite/latest
YANDEX_MODEL_CHAT=yandexgpt-5-lite/latest
```

### Миграции БД
```bash
python scripts/db_migrate.py
```

### Первый запуск артефактов модели
Если в `data/model_artifacts/` ещё нет `lsi_ml_model.cbm`:
```bash
cd ml_model && python run_pipeline.py
```
Это полный retrain, занимает 1–3 минуты. Артефакты потом скопируются автоматически
через `lsi_refresh.refresh_lsi(mode="retrain")` или вручную.

### Дашборд
```bash
streamlit run src/presentation/app.py --server.port 8501
```
Открывается на `http://localhost:8501`. Боковое меню содержит страницы M1..M5,
агрегационный LSI и LLM-чат.

### Полный live-цикл вручную
```bash
python -c "from src.application.lsi_refresh import refresh_lsi; print(refresh_lsi(mode='inference'))"
```
Парсеры → upsert `ml_model/data` → `inference.py` (predict + SHAP) →
копирование артефактов в `data/model_artifacts`.

### Метрики
```bash
python scripts/compute_metrics.py --k 6
```
Пишет `docs/METRICS_REPORT.md` и `docs/metrics_snapshot.json`.

---

## Расписание планировщика

| Время | Задача | Длительность |
|---|---|---|
| 15:00 ежедневно | `Pipeline.execute()` — снапшот LSI в БД | секунды |
| 15:30 ежедневно | `refresh_lsi(mode="inference")` | 5–20 сек |
| 16:00 по вс | `refresh_lsi(mode="retrain")` | 1–3 мин |

Запускается из [src/application/scheduler.py](src/application/scheduler.py)
поверх APScheduler. На Streamlit Cloud — отдельный воркер; локально стартует
вместе с CLI-режимом.

---

## Архитектурные принципы

- **Clean Architecture.** Слои `domain → application → infrastructure → presentation`.
  Бизнес-логика модулей (M1..M5) не знает ни про БД, ни про Streamlit, ни про
  HTTP. Можно подменить любой источник данных без правки расчёта.
- **Inference vs Retrain.** Два режима обновления LSI. Inference читает
  сохранённую модель и считает predict+SHAP за секунды; retrain делает fit
  заново раз в неделю. Контракт CSV-артефактов одинаковый.
- **Past-only нормализация.** MAD-z считается на скользящем окне 1 год
  со сдвигом на день назад — модель никогда не видит «будущее» при backtest.
- **M4 как мультипликатор.** Налоговая сезонность не входит в фичи CatBoost,
  применяется post-prediction. Соответствует требованию ТЗ и упрощает
  интерпретацию.
- **RAG с guardrails.** Чат отвечает только по выгруженным документам.
  Prompt-injection ловится регексом до обращения в LLM; отказы помечаются
  флагом и не попадают в историю при следующем вызове, чтобы не было
  индукции отказа.
- **Без хардкода ключей.** Все секреты — через `.env` / окружение.
  `.env.example` содержит плейсхолдеры, реальный `.env` под `.gitignore`.

---
## Контакты по ТЗ

ТЗ от ПСБ Казначейство: VI Весенняя школа «Информационные технологии и
искусственный интеллект», 8–13 мая 2026.
