import streamlit as st

st.set_page_config(
    page_title="RU Liquidity Sentinel",
    page_icon="📊",
    layout="wide",
)

st.title("🚀 RU Liquidity Sentinel - Quick Start")

st.markdown("""
## Добро пожаловать в систему мониторинга ликвидности рубля!

### 📚 Документация

- **[DEPLOYMENT.md](https://github.com/MathmodeHakaton/service/blob/main/DEPLOYMENT.md)** - Полная инструкция по запуску
- **[README.md](https://github.com/MathmodeHakaton/service/blob/main/README.md)** - Описание проекта

### 🚀 Быстрый старт

#### Локально (рекомендуется для разработки)
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

#### Docker (для production)
```bash
# Поднять все сервисы
make docker-up
```

### 📊 Навигация

После запуска приложения откройте левую боковую панель для перемещения между страницами:

1. **📊 Overview** - Обзор текущего состояния индекса
2. **🔍 Modules** - Детализация по модулям M1-M5
3. **📈 LSI** - Исторические данные
4. **🤖 LLM Chat** - RAG-ассистент

### 🗄️ База данных

Автоматически создается таблица `fetch_cache` для кеширования данных.

Миграции управляются через `yoyo`:
```bash
python scripts/db_migrate.py up      # Применить
python scripts/db_migrate.py status  # Статус
python scripts/db_migrate.py down    # Откатить
```

### ⚙️ Конфигурация

Скопируйте `.env.example` в `.env`:
```bash
cp .env.example .env
```

### 🐳 Docker Compose

Стек включает:
- **PostgreSQL** (БД)
- **Ollama** (LLM)
- **App** (Scheduler)
- **Streamlit** (Дашборд)

### 📞 Поддержка

Для вопросов и проблем:
- Проверьте [DEPLOYMENT.md](https://github.com/MathmodeHakaton/service/blob/main/DEPLOYMENT.md)
- Откройте Issue на GitHub

---

**Статус**: ✅ Готово к запуску
""")

# Показываем окружение
st.divider()

col1, col2, col3 = st.columns(3)

with col1:
    st.info("""
    **📦 Зависимости:**
    - streamlit >= 1.57.0
    - sqlalchemy >= 2.0.0
    - yoyo-migrations >= 7.3.0
    - plotly >= 5.24.0
    """)

with col2:
    st.info("""
    **🗄️ БД:**
    - PostgreSQL >= 13
    - yoyo для миграций
    """)

with col3:
    st.info("""
    **🐳 Docker:**
    - Docker >= 24.0
    - Docker Compose >= 2.0
    """)
