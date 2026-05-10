#!/usr/bin/env python
"""
Управление миграциями БД через yoyo
Команды:
  python scripts/db_migrate.py up          - Применить все новые миграции
  python scripts/db_migrate.py down        - Откатить последнюю миграцию
  python scripts/db_migrate.py downall     - Откатить все миграции
  python scripts/db_migrate.py status      - Показать статус миграций
"""

from config.settings import get_settings
from yoyo import get_backend, read_migrations
import sys
import os
from pathlib import Path

# Добавляем root в path
sys.path.insert(0, str(Path(__file__).parent.parent))


def get_backend_connection():
    """Получить подключение к БД"""
    settings = get_settings()
    return get_backend(settings.database_url)


def migrate_up():
    """Применить все новые миграции"""
    print("🔄 Применение миграций...")
    backend = get_backend_connection()
    migrations = read_migrations("migrations")

    with backend.lock():
        backend.apply_migrations(backend.to_apply(migrations))

    print("✅ Миграции успешно применены")


def migrate_down():
    """Откатить последнюю миграцию"""
    print("⏮️  Откат последней миграции...")
    backend = get_backend_connection()
    migrations = read_migrations("migrations")

    with backend.lock():
        backend.rollback_migrations(backend.to_rollback(migrations)[-1:])

    print("✅ Миграция откачена")


def migrate_downall():
    """Откатить все миграции"""
    confirm = input("⚠️  Это откатит ВСЕ миграции. Вы уверены? (yes/no): ")
    if confirm.lower() != "yes":
        print("❌ Отмена")
        return

    print("⏮️  Откат всех миграций...")
    backend = get_backend_connection()
    migrations = read_migrations("migrations")

    with backend.lock():
        backend.rollback_migrations(backend.to_rollback(migrations))

    print("✅ Все миграции откачены")


def migrate_status():
    """Показать статус миграций"""
    print("📊 Статус миграций:\n")
    backend = get_backend_connection()
    migrations = read_migrations("migrations")

    applied = backend.get_applied_migrations()

    print(f"Всего миграций: {len(migrations)}")
    print(f"Применено: {len(applied)}\n")

    print("Примененные миграции:")
    for mig in applied:
        print(f"  ✅ {mig}")

    pending = [m.id for m in migrations if m.id not in applied]
    if pending:
        print("\nОжидающие миграции:")
        for mig_id in pending:
            print(f"  ⏳ {mig_id}")
    else:
        print("\n✨ Все миграции применены!")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()

    try:
        if command == "up":
            migrate_up()
        elif command == "down":
            migrate_down()
        elif command == "downall":
            migrate_downall()
        elif command == "status":
            migrate_status()
        else:
            print(f"❌ Неизвестная команда: {command}")
            print(__doc__)
            sys.exit(1)
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        sys.exit(1)
