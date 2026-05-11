# PSB LSI: Weak Target + CatBoost + SHAP

Проект реализует proximity/statistics-based подход без статичных финальных весов.

## Запуск

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python run_pipeline.py --data-dir data --out-dir outputs
```

Опции:

```bash
python run_pipeline.py --early-warning-horizon 5
python run_pipeline.py --no-crisis-uplift
```

## Что происходит

1. `src/features.py` собирает дневную витрину: daily calendar, spreading месячных/событийных данных на дни, rolling MAD z-score, availability flags.
2. `src/stress_components.py` переводит признаки в атомарные stress components. Например, рост repo rate spread = стресс, падение OFZ cover = стресс.
3. `src/weak_target.py` строит непрерывный weak target. Это временная обучающая разметка, пока нет экспертных labels.
4. `src/catboost_cap.py` обучает `CatBoostRegressor` и считает SHAP-вклады.
5. `run_pipeline.py` сохраняет результаты в `outputs/`.

## Главные выходы

- `outputs/daily_features.csv` — дневная витрина признаков.
- `outputs/stress_components.csv` — атомарные стресс-компоненты.
- `outputs/weak_target_dataset.csv` — датасет с weak target.
- `outputs/lsi_timeseries.csv` — полный ряд LSI и SHAP.
- `outputs/lsi_dashboard_extract.csv` — компактная таблица для дашборда.
- `outputs/shap_module_contributions.csv` — вклад модулей по каждой дате.
- `outputs/shap_feature_contributions.csv` — вклад каждого признака по каждой дате.
- `outputs/REPORT.md` — отчет по запуску.

## Будет ли модель выдавать круглые значения?

Если target задать как грубые классы 20/50/80, да, прогнозы будут ступенчатыми. В этом проекте target сделан непрерывным: зависит от силы компонент, ширины сигнала, комбинаций модулей, persistence и early-warning горизонта. Поэтому CatBoost получает много разных значений target и прогнозирует плавную шкалу LSI.

## Как используется SHAP

SHAP не меняет LSI. Сначала модель считает `LSI`, потом SHAP объясняет, какие признаки и модули подняли или снизили прогноз. Для дашборда SHAP группируется по M1, M2, M3, M4, M5, COHERENCE, INTERACTION.

## LLM-комментатор (Yandex AI Studio)

Бонус-модуль из ТЗ: при каждом пересчёте LSI облачная модель пишет аналитический комментарий (4-6 предложений) на основе текущего значения, вкладов модулей, активных каналов стресса и календарного контекста.

### Настройка

1. Зарегистрироваться в [Yandex AI Studio](https://yandex.cloud/ai-studio), получить **service-account API-key** и **folder ID**.
2. Прописать в окружении:
   ```bash
   # Linux/macOS
   export YANDEX_API_KEY="AQVN..."
   export YANDEX_FOLDER_ID="b1g..."

   # Windows PowerShell
   $env:YANDEX_API_KEY = "AQVN..."
   $env:YANDEX_FOLDER_ID = "b1g..."
   ```
3. Запустить как обычно — `run_pipeline.py` сам подтянет ключи и сохранит `outputs/llm_commentary.md`. Если ключей нет, шаг пропустится без падения.

### Отдельный запуск

Перегенерировать комментарий без полного пересчёта LSI:

```bash
# на последний валидный день
python comment.py

# на конкретную дату
python comment.py --date 2022-02-28 --out outputs/commentary_feb_2022.md

# более сильная (дорогая) модель
python comment.py --model yandexgpt
```

### Что внутри

`src/llm_commentator.py` собирает структурированный контекст из `lsi_timeseries.csv` (вклады SHAP, динамика к неделе/месяцу, активные каналы, налоговый календарь), формирует промпт и вызывает `https://llm.api.cloud.yandex.net/foundationModels/v1/completion`. System-prompt задаёт жанр («старший аналитик казначейства»), требования (только факты из контекста, без выдумок) и структуру ответа.

Каждый сохранённый комментарий в `outputs/llm_commentary.md` содержит **полный audit trail**: оригинальный JSON-контекст и user-prompt, чтобы можно было воспроизвести/перепроверить ответ.
