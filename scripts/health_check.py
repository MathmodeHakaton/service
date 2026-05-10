#!/usr/bin/env python
"""
Health check - проверка что всё готово к запуску
"""

import sys
from pathlib import Path
from typing import Tuple

# Добавляем root в path
sys.path.insert(0, str(Path(__file__).parent.parent))


def check_environment() -> Tuple[bool, list]:
    """Проверяем переменные окружения"""
    issues = []

    try:
        from config.settings import get_settings
        settings = get_settings()

        # Проверяем критические параметры
        if "postgresql" not in settings.database_url:
            issues.append("⚠️  DATABASE_URL не содержит PostgreSQL")

        if not settings.database_url:
            issues.append("❌ DATABASE_URL не установлен")

    except Exception as e:
        issues.append(f"❌ Ошибка загрузки конфигурации: {e}")

    return len(issues) == 0, issues


def check_database() -> Tuple[bool, list]:
    """Проверяем подключение к БД"""
    issues = []

    try:
        from src.infrastructure.storage.db.engine import get_session

        session = next(get_session())
        # Простой тест
        session.execute("SELECT 1")
        session.close()

    except Exception as e:
        issues.append(f"❌ Ошибка подключения к БД: {e}")
        issues.append(
            "   → Убедитесь что PostgreSQL запущен и DATABASE_URL верен")
        return False, issues

    return True, []


def check_migrations() -> Tuple[bool, list]:
    """Проверяем миграции"""
    issues = []

    try:
        from src.infrastructure.storage.db.engine import get_session

        session = next(get_session())
        # Проверяем что таблица кэша существует
        result = session.execute(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'fetch_cache')"
        )
        table_exists = result.scalar()
        session.close()

        if not table_exists:
            issues.append("⚠️  Таблица fetch_cache не существует")
            issues.append("   → Запустите: python scripts/db_migrate.py up")

    except Exception as e:
        issues.append(f"❌ Ошибка проверки миграций: {e}")
        return False, issues

    return len(issues) == 0, issues


def check_dependencies() -> Tuple[bool, list]:
    """Проверяем критические зависимости"""
    issues = []
    required = [
        "streamlit",
        "sqlalchemy",
        "pandas",
        "pydantic",
        "yoyo",
        "psycopg2",
        "plotly",
    ]

    for package in required:
        try:
            __import__(package)
        except ImportError:
            issues.append(f"❌ Пакет {package} не установлен")

    return len(issues) == 0, issues


def main():
    """Основная функция проверки"""

    print("=" * 60)
    print("🏥 RU Liquidity Sentinel - Health Check")
    print("=" * 60)

    checks = [
        ("📦 Зависимости", check_dependencies),
        ("🔧 Конфигурация", check_environment),
        ("🗄️  БД Подключение", check_database),
        ("📋 Миграции", check_migrations),
    ]

    all_ok = True
    for name, check_func in checks:
        print(f"\n{name}:")
        ok, issues = check_func()

        if ok:
            print("  ✅ OK")
        else:
            all_ok = False
            for issue in issues:
                print(f"  {issue}")

    print("\n" + "=" * 60)

    if all_ok:
        print("✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ - готово к запуску!")
        print("\nЗапустите одну из команд:")
        print("  make run-dashboard        # Локальный дашборд")
        print("  make docker-up            # Docker стек")
        print("  uv run streamlit run ...  # Streamlit напрямую")
    else:
        print("⚠️  ОБНАРУЖЕНЫ ПРОБЛЕМЫ - исправьте ошибки выше")
        print("\nПроверьте документацию:")
        print("  DEPLOYMENT.md - Локальный запуск")
        print("  STREAMLIT_CLOUD_DEPLOYMENT.md - Облачный деплой")

    print("=" * 60)

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
