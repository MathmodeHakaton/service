"""
Pipeline: fetch → cache → compute → aggregate
"""

from typing import List
from src.infrastructure.fetchers.fake_fetcher import FakeFetcher
from src.infrastructure.storage.cache import ParquetCache
from src.domain.modules.m1_reserves import M1Reserves
from src.domain.modules.m2_repo import M2Repo
from src.domain.modules.m3_ofz import M3OFZ
from src.domain.modules.m4_tax import M4Tax
from src.domain.modules.m5_treasury import M5Treasury
from src.domain.aggregation.lsi_engine import LSIEngine
from src.domain.models.lsi_result import LSIResult


class Pipeline:
    """Основной пайплайн анализа ликвидности"""

    def __init__(self):
        self.fetcher = FakeFetcher()
        self.cache = ParquetCache()
        self.modules = [
            M1Reserves(),
            M2Repo(),
            M3OFZ(),
            M4Tax(),
            M5Treasury(),
        ]
        self.engine = LSIEngine()

    def execute(self) -> LSIResult:
        """Выполнить полный пайплайн"""

        # Этап 1: Получить данные
        data = self._fetch_data()

        # Этап 2: Вычислить сигналы модулей
        signals = self._compute_module_signals(data)

        # Этап 3: Агрегировать в LSI
        lsi_result = self.engine.compute(signals)

        return lsi_result

    def _fetch_data(self) -> dict:
        """Получить данные с кэшированием"""
        cache_key = "latest_data"

        # Проверить кэш
        cached_data = self.cache.get(cache_key)
        if cached_data:
            return cached_data

        # Получить свежие данные
        result = self.fetcher.fetch()
        if result.status == "success":
            self.cache.set(cache_key, result.data)
            return result.data

        return {}

    def _compute_module_signals(self, data: dict):
        """Вычислить сигналы от модулей"""
        signals = []
        for module in self.modules:
            signal = module.compute(data)
            signals.append(signal)
        return signals
