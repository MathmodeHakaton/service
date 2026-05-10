# Запуск RU Liquidity Sentinel

## 🚀 Быстрый старт

### Вариант 1: Локальная разработка (самый простой)

```bash
# 1. Установить зависимости
make setup

# 2. Поднять БД
make db-up

# 3. Применить миграции
make db-migrate

# 4. Запустить дашборд
make run-dashboard
```

Дашборд будет доступен на **http://localhost:8501**

---

### Вариант 2: Docker (production)

```bash
# Поднять все сервисы (PostgreSQL, Ollama, App scheduler, Streamlit)
make docker-up
```

Это запустит:
- 📊 **Streamlit Dashboard**: http://localhost:8501
- 🐘 **PostgreSQL**: localhost:5432
- 🦙 **Ollama**: localhost:11434
- ⏱️ **App Scheduler**: работает в фоне

Чтобы остановить:
```bash
make docker-down
```

---

## 📋 Команды управления

### Базовые команды
```bash
make help              # Показать все доступные команды
make setup             # Установить зависимости
make clean             # Очистить кэши
```

### БД и миграции
```bash
make db-up             # Поднять PostgreSQL контейнер
make db-down           # Остановить PostgreSQL
make db-migrate        # Применить все миграции
make db-rollback       # Откатить последнюю миграцию
make db-reset          # Откатить все миграции и пересоздать БД
make db-status         # Показать статус миграций
```

### Запуск приложения
```bash
make run               # Выполнить пайплайн один раз
make run-schedule      # Запустить планировщик (ежедневно в 15:00)
make run-backtest      # Запустить backtesting (2014-2023)
make run-dashboard     # Запустить Streamlit дашборд
```

### Docker
```bash
make docker-build      # Собрать образы
make docker-up         # Поднять все сервисы
make docker-down       # Остановить все сервисы
make docker-logs       # Показать логи всех контейнеров
make docker-logs-app   # Логи приложения
make docker-logs-db    # Логи БД
```

---

## 🔧 Конфигурация

### Переменные окружения
Скопируйте `.env.example` в `.env` и отредактируйте при необходимости:

```bash
cp .env.example .env
```

Основные переменные:
```env
DATABASE_URL=postgresql://user:password@localhost:5432/ru_liquidity_sentinel
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama2
LOG_LEVEL=INFO
```

### Streamlit конфигурация
Находится в `.streamlit/config.toml`:
- Порт: 8501
- Тема: сине-белая
- Сбор статистики отключен

---

## 🗄️ Миграции БД

### Структура миграций
```
migrations/
├── 0001_add_fetch_cache.sql         # Создание таблицы кэша
└── 0001_add_fetch_cache.rollback.sql # Откат
```

### Применение миграций

**Автоматически** при запуске Docker:
```bash
make docker-up  # Миграции применяются автоматически
```

**Вручную**:
```bash
# Применить все
python scripts/db_migrate.py up

# Откатить последнюю
python scripts/db_migrate.py down

# Откатить все
python scripts/db_migrate.py downall

# Показать статус
python scripts/db_migrate.py status
```

---

## 📊 Структура приложения

```
.
├── main.py                 # Точка входа (typer CLI)
├── run.py                 # Удобный скрипт запуска
├── Dockerfile             # Образ для приложения
├── Dockerfile.streamlit   # Образ для Streamlit
├── docker-compose.yml     # Оркестрация сервисов
├── Makefile              # Удобные команды
│
├── src/
│   ├── application/       # Бизнес-логика
│   │   ├── pipeline.py   # Главный пайплайн
│   │   ├── scheduler.py  # Планировщик (APScheduler)
│   │   └── backtest.py   # Backtesting
│   ├── domain/           # Доменная логика
│   │   ├── modules/      # M1-M5 модули
│   │   ├── aggregation/  # LSI engine
│   │   └── llm/          # RAG и LLM интеграция
│   ├── infrastructure/   # Техническая часть
│   │   ├── fetchers/     # Получение данных (с кешированием)
│   │   └── storage/      # БД и кеширование
│   └── presentation/     # Streamlit frontend
│       ├── app.py        # Главная страница
│       ├── data_loader.py # Загрузчик данных
│       └── pages/        # Страницы дашборда
│
├── config/
│   ├── settings.py       # Конфигурация (Pydantic)
│   └── constants.py      # Константы
│
├── migrations/           # SQL миграции (yoyo)
├── scripts/
│   └── db_migrate.py     # Управление миграциями
├── tests/                # Тесты
└── cache/                # Локальный кеш данных
```

---

## 🐳 Docker Compose структура

### Сервисы

1. **postgres** (PostgreSQL 16)
   - БД для хранения кэша и результатов
   - Том: `postgres_data`
   - Порт: 5432
   - Healthcheck: 5 проверок, интервал 10s

2. **ollama** (Ollama LLM)
   - Локальные LLM модели
   - Том: `ollama_data`
   - Порт: 11434

3. **app** (Приложение)
   - Выполняет миграции
   - Запускает планировщик
   - Зависит от: postgres

4. **streamlit** (Дашборд)
   - Streamlit фронтенд
   - Порт: 8501
   - Зависит от: postgres, app

### Сеть
Все сервисы подключены к сети `ru_liquidity` для взаимодействия.

---

## 📈 Workflow для разработки

### Начальная настройка
```bash
make init      # setup + db-up + db-migrate
```

### Разработка с дашбордом
```bash
make dev       # db-up + db-migrate + run-dashboard
```

### Проверка миграций
```bash
make db-status
```

### Откат и пересоздание
```bash
make db-reset
```

---

## 🚨 Troubleshooting

### PostgreSQL не запускается
```bash
# Проверить статус
docker-compose ps postgres

# Посмотреть логи
make docker-logs-db

# Полный перезапуск
make docker-down
docker volume rm ru-liquidity-sentinel_postgres_data
make docker-up
```

### Миграции не применились
```bash
# Проверить статус
make db-status

# Применить вручную
make db-migrate

# Откатить все и пересоздать
make db-reset
```

### Streamlit не открывается
```bash
# Проверить логи
make docker-logs-streamlit

# Перезапустить
docker-compose restart streamlit
```

---

## 📝 Примечания

- **yoyo** - инструмент для миграций (вместо Alembic)
- **Кеширование** - данные кешируются в БД (`fetch_cache`)
- **TTL** - время жизни кеша разное для каждого источника (6-168 часов)
- **Планировщик** - запускается ежедневно в 15:00 (после закрытия рынка ЦБ)
- **Streamlit Cloud** - поддерживается для production деплоя

---

## 🚀 Production Deployment (Streamlit Cloud)

1. Загрузить проект на GitHub
2. Подключить в [Streamlit Cloud](https://share.streamlit.io)
3. Указать команду: `streamlit run src/presentation/app.py`
4. Добавить переменные окружения в settings
5. БД на managed PostgreSQL (Supabase, Railway и т.п.)

---

## 📚 Полезные ссылки

- [yoyo-migrations docs](https://ollycross.github.io/yoyo/)
- [Streamlit docs](https://docs.streamlit.io)
- [Docker Compose reference](https://docs.docker.com/compose/compose-file/)
- [SQLAlchemy docs](https://docs.sqlalchemy.org/)
