# Отчёт: live-пайплайн LSI + Yandex LLM + RAG-чат

## TL;DR
- Бэк (`application/pipeline.py`, фетчеры, домены, БД-кэш) не тронут.
- Новая ML-крышка живёт в [ml_model/](../ml_model/) (CatBoost + SHAP).
- Сервис умеет обновлять артефакты модели «на свежих данных» — кнопкой на
  странице LSI или ежедневным cron-джобом.
- Авто-комментарий и RAG-чат используют Yandex AI Studio через
  **OpenAI-совместимый endpoint** (`openai` SDK, `chat.completions`).
  Модель: `yandexgpt-5-lite/latest` по умолчанию, переключается в сайдбаре чата.
- Чат **отвечает строго по документам**: если вопрос вне темы LSI/M1–M5/денежный
  рынок РФ или факт отсутствует в RAG-контексте — модель обязана отказаться
  стандартной фразой (правило прошито в system-промпте).

## Два режима обновления: inference и retrain

Daily-обновление ≠ переобучение. Разделено:

| Режим | Что делает | Скорость | Когда |
|---|---|---|---|
| **inference** | Грузит сохранённую `lsi_ml_model.cbm` + `lsi_ml_metadata.joblib`, строит фичи, делает `predict` + SHAP, Калман+гистерезис, backtest. **fit() не вызывается.** | 5–20 секунд | Кнопка «🔄 Обновить LSI» · cron 15:30 ежедневно |
| **retrain** | Полный `run_pipeline.py`: TimeSeriesCV, fit CatBoost, sensitivity ±20%, SHAP, сохраняет новую `.cbm` и `feature_importance.csv`. | 1–3 минуты | Кнопка «🧠 Переобучить» · cron вс 16:00 |

Inference перезаписывает `lsi_timeseries.csv`, `lsi_dashboard_extract.csv`,
`backtest_crisis_episodes.csv`. Файлы модели (`.cbm`, `.joblib`,
`feature_importance.csv`, `module_importance_catboost.csv`) **не трогаются** —
они от последнего retrain. Если на момент inference артефактов модели нет
(первый запуск, ручное удаление) — `refresh_lsi(mode="inference")` авто-фолбэкает
на retrain и пишет в лог `mode=retrain`.

[ml_model/inference.py](../ml_model/inference.py) повторяет шаги 1, 4, 5, 6
из `train_ml_lsi` (без 2 — нет target-derivation, без 3 — нет fit, без 7 —
нет sensitivity). Кодовый contract с CSV-артефактами идентичен.

## Архитектура live-обновления

```
[ Streamlit "🔄 Обновить" ] ── или ── [ APScheduler 15:30 ]
            │
            ▼
src/application/lsi_refresh.refresh_lsi()
            │
   1. Pipeline.execute_full()                     ← src/application/pipeline.py
        — fetchers (ЦБ/Минфин/ФНС/Росказна) c кэшем БД
            │
   2. upsert_ml_inputs(raw_data)                  ← src/application/lsi_refresh.py
        — merge по date в ml_model/data/*.csv
        — НЕ трогает m3_ofz_full.csv, m4_tax_calendar.csv, m5_sors_federal_funds.csv
            │
   3. subprocess: python run_pipeline.py          ← ml_model/run_pipeline.py
        — features + stress_components + train_ml_lsi + SHAP + backtest
            │
   4. copy ml_model/outputs/* → data/model_artifacts/
            │
            ▼
Дашборд (страница LSI) видит свежие CSV (cache TTL=10 мин, кнопка делает
st.cache_data.clear() и st.rerun()).
```

### Почему именно так

- **CatBoost запускается отдельным процессом.** Импортировать `ml_model` в
  основной процесс Streamlit рискованно: модель сейчас переобучается на каждом
  прогоне (так задумано в `run_pipeline.py`), это десятки секунд CPU и
  свой `joblib` лок. Subprocess — это изоляция импорта, чистый рестарт и
  гарантия, что Streamlit-страница не упадёт от ошибки в ML-коде.
- **Upsert вместо перезаписи CSV.** В `ml_model/data/` лежат снапшоты с 2004
  года — это вся история, на которой модель калибруется. Парсеры сервиса
  возвращают только новый кусок. Делаем `concat(old, fresh).drop_duplicates(date, keep='last')`
  — старая история сохраняется, свежие даты накатываются.
- **OFZ / tax / SORS не апдейтятся live.** У парсера Минфина другая схема
  колонок (кириллица в snapshot vs английский в парсере) — мэппинг 1:1 не
  безопасен. Налоговый календарь меняется раз в год. SORS Росказны
  недоступен через SSL (см. `roskazna.py`). Эти три файла остаются
  снапшотами; всё остальное (M1/M2/M5) — живые.
- **Кэш Streamlit `ttl=600`** + явный сброс после refresh — баланс между
  быстротой отрисовки и видимостью свежих данных.

## Изменения по файлам

| Файл | Что изменилось |
|---|---|
| [config/settings.py](../config/settings.py) | Добавлены `yandex_api_key`, `yandex_folder_id`, `yandex_model_commentary`, `yandex_model_chat` (читаются из ENV/.env) |
| [src/application/lsi_refresh.py](../src/application/lsi_refresh.py) | **NEW.** Sync + run_pipeline + copy artifacts |
| [src/application/scheduler.py](../src/application/scheduler.py) | Добавлен job `daily_lsi_ml_refresh` (15:30) |
| [src/presentation/pages/6_LSI.py](../src/presentation/pages/6_LSI.py) | Кнопка «🔄 Обновить», auto-commentary через YandexGPT |
| [src/presentation/pages/7_LLM.py](../src/presentation/pages/7_LLM.py) | **NEW.** RAG-чат с YandexGPT |
| [src/presentation/rag/knowledge_base.py](../src/presentation/rag/knowledge_base.py) | **NEW.** Чанки: статика ТЗ + динамика из артефактов |
| [src/presentation/rag/retriever.py](../src/presentation/rag/retriever.py) | **NEW.** Гибридный скоринг: BM25-lite + теги |
| [src/presentation/rag/chat_llm.py](../src/presentation/rag/chat_llm.py) | **NEW.** System-промпт RAG-чата + жёсткое правило «только по документам» |
| [src/presentation/rag/yandex_client.py](../src/presentation/rag/yandex_client.py) | **NEW.** Обёртка над `openai.OpenAI` (Yandex base_url + project=folder) |
| [src/presentation/rag/commentary_prompt.py](../src/presentation/rag/commentary_prompt.py) | **NEW.** Сборка контекста и user-prompt для авто-комментария |

### Удалено
- `src/presentation/llm_commentary.py` — Ollama-обёртка не нужна (Yandex).
- `src/domain/llm/local_model.py` — Ollama LocalLLM, не используется.
- `src/domain/llm/rag_retriever.py` — placeholder с DB-историей, не используется.
- `data/model_artifacts/charts/` — pre-rendered PNG, мы рисуем в plotly.
- `data/model_artifacts/shap_module_contributions.csv` — не читается кодом.
- `data/model_artifacts/catboost_lsi_model.cbm` — дубль `lsi_ml_model.cbm`.
- Пакет `ollama` из `requirements.txt`.

## RAG: как устроен

**Knowledge base** (`build_knowledge_base()`):
- 7 статических чанков: 5 модулей + методология LSI + пороги статуса
  (короткие, на основе ТЗ, со ссылками на колонки фич — чтобы LLM могла
  цитировать `m3_nedospros_flag` и т.п.).
- Динамические чанки из `data/model_artifacts/`:
  - `latest` — текущий LSI, контрибуции, статус.
  - `yr_YYYY` — годовой срез (mean/max LSI, % красных дней, доминирующий
    модуль в пике). По одному чанку на год.
  - `top_days` — топ-12 стрессовых дней.
  - `ep_<episode>` — backtest по фев-2022 / авг-2023.
  - `mod_imp`, `feat_imp` — глобальная важность.
  - `tax_upcoming` — ближайшие налоговые даты (если pipeline вернул календарь).

**Retrieval** — `retrieve(query, chunks, k)`:
- BM25-lite: token overlap × IDF (вычисляется по KB).
- Буст +4 за каждое совпадение тега (год / модуль / ключевое слово).
- Чанк `latest` всегда сверху — system-context для любого вопроса.
- top-k (default 6) идёт в system-prompt.

**Generation** — `yandex_client.chat()` через `openai.OpenAI`:
```python
client = OpenAI(
    api_key=settings.yandex_api_key,
    base_url="https://ai.api.cloud.yandex.net/v1",
    project=settings.yandex_folder_id,
)
client.chat.completions.create(
    model=f"gpt://{folder_id}/{name}/{version}",  # gpt://b1g.../yandexgpt-5-lite/latest
    messages=[{"role": "system", "content": ...},
              *history,
              {"role": "user", "content": prompt}],
)
```
- System prompt с жёсткими правилами:
  1. Отвечать только про LSI / M1–M5 / денежный рынок РФ.
  2. Off-topic → шаблонный отказ «Я отвечаю только по данным системы LSI.»
  3. Факт по теме но нет в RAG-контексте → «В выгруженных данных системы такой
     информации нет.»
  4. Цифры/даты — дословно из контекста, без округлений и интерполяций.
- Messages: вся история из `st.session_state["chat_history"]`.
- Модель/temperature — сайдбар.

Почему не embeddings? KB сейчас ~30 коротких чанков, гибридный скоринг +
теги хорошо отрабатывает финансовые вопросы с явными датами/модулями.
Если KB вырастет — заменим скоринг на Yandex `text-search-doc` /
`text-search-query` без изменения контракта retriever.

## Запуск

```powershell
# 1. Ключи Yandex AI Studio (.env в корне репо или ENV)
YANDEX_API_KEY=AQVN...
YANDEX_FOLDER_ID=b1g...

# 2. Зависимости ml_model (включает catboost, scipy, sklearn)
pip install -r ml_model/requirements.txt
# и зависимости основного проекта
pip install -r requirements.txt

# 3. Первый полный прогон артефактов (опционально — кнопкой «Обновить» из UI тоже работает)
python -c "from src.application.lsi_refresh import refresh_lsi; print(refresh_lsi())"

# 4. Дашборд
streamlit run src/presentation/app.py --server.port 8501
```

В Docker-compose шедулер запускается отдельным сервисом — он подцепит
ежедневный refresh автоматически.

## Что осталось / возможные улучшения

- **OFZ live-апдейт.** Нужен адаптер `Minfin parser → m3_ofz_full.csv`
  (кириллические колонки). Не сделано из-за риска поломать схему.
- **Embeddings RAG.** Когда KB вырастет — переехать на Yandex embeddings.
- **Уведомления.** При переходе LSI в красную зону можно отправлять алерт
  (slack/email) — точка расширения в `scheduler._daily_inference`.
- **Инкрементальный inference.** Сейчас inference считает фичи на всей истории
  каждый раз (~5 сек). Можно кэшировать `daily_features.csv` и дописывать
  только новые даты — на горизонте оптимизации, не критично.
