"""
Инициализация для Streamlit Cloud
Автоматически применяет миграции при первом запуске
"""

import streamlit as st
import psycopg2
from urllib.parse import urlparse
import sys
from pathlib import Path

# Добавляем root в path
sys.path.insert(0, str(Path(__file__).parent.parent))


def parse_db_url(db_url: str) -> dict:
    """Парсим DATABASE_URL"""
    parsed = urlparse(db_url)
    return {
        "dbname": parsed.path[1:],
        "user": parsed.username,
        "password": parsed.password,
        "host": parsed.hostname,
        "port": parsed.port or 5432,
    }


def check_table_exists(conn, table_name: str) -> bool:
    """Проверяем что таблица существует"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT 1 FROM information_schema.tables
                   WHERE table_name = %s""",
                (table_name,),
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def apply_migrations(db_url: str) -> bool:
    """Применяем миграции"""
    try:
        conn_params = parse_db_url(db_url)
        conn = psycopg2.connect(**conn_params)

        # Проверяем существует ли таблица
        if check_table_exists(conn, "fetch_cache"):
            print("✅ Таблица fetch_cache уже существует")
            conn.close()
            return True

        # Применяем миграцию
        with conn.cursor() as cur:
            migration_file = Path(__file__).parent.parent / \
                "migrations" / "0001_add_fetch_cache.sql"
            with open(migration_file) as f:
                cur.execute(f.read())
            conn.commit()

        print("✅ Миграции успешно применены")
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Ошибка при применении миграций: {e}")
        return False


@st.cache_resource
def initialize_database():
    """Инициализируем БД при первом запуске"""
    try:
        db_url = st.secrets.get("DATABASE_URL")
        if not db_url:
            st.error("❌ DATABASE_URL не найден в Streamlit Secrets")
            return False

        return apply_migrations(db_url)
    except Exception as e:
        st.error(f"❌ Ошибка инициализации: {e}")
        return False


def main():
    st.set_page_config(page_title="Streamlit Cloud Init", layout="centered")
    st.title("🚀 Инициализация Streamlit Cloud")

    st.markdown("""
    Этот скрипт автоматически применяет миграции БД
    при первом запуске на Streamlit Cloud.
    """)

    if st.button("🔄 Применить миграции"):
        with st.spinner("⏳ Применение миграций..."):
            if initialize_database():
                st.success("✅ Миграции успешно применены!")
                st.info("""
                Теперь вы можете перейти на главную страницу приложения:

                `streamlit run src/presentation/app.py`
                """)
            else:
                st.error("❌ Ошибка при применении миграций")

    st.divider()

    with st.expander("📋 Инструкции"):
        st.markdown("""
        ### Шаги деплоя на Streamlit Cloud:

        1. Загрузьте проект на GitHub
        2. Создайте PostgreSQL БД (Supabase, Railway и т.п.)
        3. На Streamlit Cloud откройте App Settings → Secrets
        4. Добавьте:
           ```toml
           DATABASE_URL = "postgresql://..."
           OLLAMA_BASE_URL = "http://..."
           ```
        5. Запустите этот скрипт один раз
        6. Затем откройте основное приложение

        Подробнее: [STREAMLIT_CLOUD_DEPLOYMENT.md](STREAMLIT_CLOUD_DEPLOYMENT.md)
        """)


if __name__ == "__main__":
    main()
