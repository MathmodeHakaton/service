#!/usr/bin/env python3
"""
Скрипт для очистки fetch_cache при обновлении структуры FetcherResult.
Удаляет все закэшированные данные, чтобы при следующем запуске
они были переполучены и сохранены с новой структурой.
"""

import logging
from sqlalchemy import create_engine, text
from src.infrastructure.storage.db.engine import get_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clear_cache():
    """Очистить таблицу fetch_cache"""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("DELETE FROM fetch_cache"))
            conn.commit()
            logger.info(f"✓ Удалено {result.rowcount} записей из fetch_cache")
    except Exception as e:
        logger.error(f"✗ Ошибка при очистке кеша: {e}")
        raise


if __name__ == "__main__":
    clear_cache()
