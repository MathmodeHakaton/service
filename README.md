# RU Liquidity Sentinel

Система мониторинга ликвидности рубля на основе анализа ключевых индикаторов из данных ЦБ РФ, Минфина, Росказны и ФНС.

## Архитектура

Проект построен на принципах Clean Architecture:

- **Infrastructure Layer**: Fetchers, Storage, HTTP Client
- **Domain Layer**: Entities, Modules, Normalization, Aggregation, LLM
- **Application Layer**: Pipeline, Backtest, Scheduler
- **Presentation Layer**: Streamlit UI

## Установка

```bash
# Создать виртуальное окружение
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Установить зависимости
pip install -e .

# Установить зависимости для разработки
pip install -e ".[dev]"
```

## Конфигурация

Скопируйте `.env.example` в `.env` и заполните необходимые значения:

```bash
cp .env.example .env
```

## Запуск

### CLI

```bash
# Выполнить пайплайн один раз
sentinel pipeline run

# Запустить планировщик
sentinel scheduler start

# Запустить backtesting
sentinel backtest --start-year 2014 --end-year 2023
```

### Streamlit UI

```bash
streamlit run src/presentation/app.py
```

## Docker

```bash
# Запустить PostgreSQL и Ollama
docker-compose up -d

# Запустить миграции
yoyo apply --database $DATABASE_URL migrations/
```

## Разработка

```bash
# Запустить тесты
pytest

# С покрытием кода
pytest --cov=src

# Форматирование кода
black .
isort .

# Проверка типов
mypy src/
```
