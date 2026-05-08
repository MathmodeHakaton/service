"""
Кэш на базе Parquet файлов с поддержкой TTL
"""

import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
import pandas as pd


class ParquetCache:
    """Кэш с использованием Parquet файлов"""

    def __init__(self, cache_dir: str = "./cache", ttl_hours: int = 24):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.ttl = timedelta(hours=ttl_hours)

    def get(self, key: str) -> Optional[Any]:
        """Получить значение из кэша"""
        cache_file = self.cache_dir / f"{key}.parquet"

        if not cache_file.exists():
            return None

        # Проверить TTL
        file_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
        if file_age > self.ttl:
            cache_file.unlink()
            return None

        try:
            df = pd.read_parquet(cache_file)
            return df.to_dict(orient="records")
        except Exception:
            return None

    def set(self, key: str, value: Any) -> None:
        """Сохранить значение в кэш"""
        cache_file = self.cache_dir / f"{key}.parquet"

        try:
            if isinstance(value, list):
                df = pd.DataFrame(value)
            else:
                df = pd.DataFrame([value])

            df.to_parquet(cache_file, index=False)
        except Exception:
            pass

    def clear(self) -> None:
        """Очистить весь кэш"""
        for file in self.cache_dir.glob("*.parquet"):
            file.unlink()
