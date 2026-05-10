# Streamlit Cloud Deployment

## 📋 Требования

- GitHub аккаунт с загруженным проектом
- PostgreSQL база (Supabase, Railway, Render и т.п.)
- Streamlit Cloud аккаунт

## 🚀 Шаг 1: Подготовка GitHub

1. Загрузите проект на GitHub:
```bash
git push origin main
```

2. Убедитесь что в `.gitignore` находятся:
```
.env
.env.local
__pycache__/
*.pyc
cache/
logs/
postgres_data/
```

## 🚀 Шаг 2: Создать PostgreSQL базу

Используйте один из сервисов:

### Вариант A: Supabase (рекомендуется)
1. Создайте проект на https://supabase.com
2. Скопируйте строку подключения (Project Settings → Database)
3. Создайте миграцию вручную в SQL Editor или через pgAdmin

### Вариант B: Railway
1. Создайте новый проект на https://railway.app
2. Добавьте PostgreSQL
3. Скопируйте Database URL

### Вариант C: Render
1. Создайте PostgreSQL базу на https://render.com
2. Скопируйте External Database URL

## 🚀 Шаг 3: Применить миграции в облаке

### Вариант 1: Streamlit Secrets + Python скрипт

Создайте приватный скрипт `scripts/cloud_init.py`:

```python
import streamlit as st
import psycopg2
from psycopg2 import sql

# Получить DATABASE_URL из Streamlit Secrets
db_url = st.secrets["DATABASE_URL"]

# Парсим URL
from urllib.parse import urlparse
parsed = urlparse(db_url)

conn = psycopg2.connect(
    dbname=parsed.path[1:],
    user=parsed.username,
    password=parsed.password,
    host=parsed.hostname,
    port=parsed.port
)

# Применить миграцию
with conn.cursor() as cur:
    with open("migrations/0001_add_fetch_cache.sql") as f:
        cur.execute(f.read())
    conn.commit()

st.success("✅ Миграции применены!")
```

Запустите: `streamlit run scripts/cloud_init.py`

### Вариант 2: pgAdmin онлайн
1. Откройте https://www.pgadmin.org/deployment/docker/
2. Подключитесь к вашей БД
3. Скопируйте содержимое `migrations/0001_add_fetch_cache.sql` и выполните

### Вариант 3: psql из терминала
```bash
PGPASSWORD=your_password psql -h your_host -U your_user -d your_db < migrations/0001_add_fetch_cache.sql
```

## 🚀 Шаг 4: Конфигурация Streamlit Cloud

### Создание проекта
1. Перейдите на https://share.streamlit.io
2. Нажмите "New app"
3. Выберите ваш GitHub репозиторий
4. Ветка: `main`
5. Main file path: `src/presentation/app.py`

### Добавление Secrets
1. Откройте Advanced settings
2. Нажмите "Secrets"
3. Добавьте переменные окружения:

```toml
# .streamlit/secrets.toml (на Streamlit Cloud)

DATABASE_URL = "postgresql://user:password@host:5432/database"
OLLAMA_BASE_URL = "http://your-ollama-server:11434"
OLLAMA_MODEL = "llama2"
LOG_LEVEL = "INFO"
```

## 📝 Файлы для облака

Убедитесь что эти файлы в репозитории:
```
.streamlit/
├── config.toml          # Конфигурация (уже есть)
└── secrets.toml         # ⚠️ НЕ загружать на GitHub!

requirements.txt         # Зависимости
src/presentation/app.py  # Точка входа
```

## 🚨 Важно: Безопасность

### НЕ ЗАГРУЖАЙТЕ эти файлы на GitHub:
```
.env
.streamlit/secrets.toml
```

### Используйте Streamlit Secrets для sensitive данных:
```python
import streamlit as st

db_url = st.secrets["DATABASE_URL"]
```

## 🔄 CI/CD Pipeline (опционально)

Создайте `.github/workflows/deploy.yml` для автоматизации:

```yaml
name: Deploy to Streamlit Cloud

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to Streamlit
        run: |
          curl https://share.streamlit.io/api/v1/deploy \
            -X POST \
            -H "Authorization: Bearer ${{ secrets.STREAMLIT_AUTH_TOKEN }}" \
            -F "repository=${{ github.repository }}"
```

## 📊 Мониторинг

После деплоя:
1. Проверьте логи Streamlit Cloud
2. Тестируйте функционал
3. Мониторьте производительность через Analytics

## 🐛 Troubleshooting

### Ошибка подключения к БД
```
Error: could not connect to server: Connection refused
```

**Решение:**
- Проверьте DATABASE_URL в secrets
- Убедитесь что БД сервер доступен (не за firewall)
- Проверьте права доступа

### Миграции не применились
```
relation "fetch_cache" does not exist
```

**Решение:**
- Примените миграции вручную в pgAdmin
- Используйте `scripts/cloud_init.py` для инициализации

### Ollama недоступна
```
Error: Connection refused at localhost:11434
```

**Решение:**
- Используйте manage Ollama сервер (например через Docker Hub)
- Или отключите LLM функции

## 📚 Дополнительные ресурсы

- [Streamlit Cloud Docs](https://docs.streamlit.io/streamlit-cloud)
- [Streamlit Secrets Guide](https://docs.streamlit.io/streamlit-cloud/get-started/deploy-an-app/secrets-management)
- [Supabase Docs](https://supabase.com/docs)
- [Railway Docs](https://docs.railway.app/)

---

**Среднее время деплоя**: ~5 минут
**Стоимость**: Бесплатно (Streamlit Community)
