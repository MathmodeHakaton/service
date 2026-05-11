"""
Pipeline: fetch → cache → compute → aggregate

Каждый модуль возвращает pd.DataFrame с признаками по ТЗ.
Pipeline собирает latest-строку из каждого DataFrame и передаёт в LSIEngine.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any

import pandas as pd

from src.infrastructure.fetchers.cbr      import CBRFetcher
from src.infrastructure.fetchers.minfin   import MinfinFetcher
from src.infrastructure.fetchers.fns      import FNSFetcher
from src.infrastructure.fetchers.roskazna import RoskaznaFetcher
from src.infrastructure.storage.cache     import ParquetCache
from src.domain.modules.m1_reserves       import M1Reserves
from src.domain.modules.m2_repo           import M2Repo
from src.domain.modules.m3_ofz            import M3OFZ
from src.domain.modules.m4_tax            import M4Tax
from src.domain.modules.m5_treasury       import M5Treasury
from src.domain.aggregation.lsi_engine    import LSIEngine
from src.domain.models.lsi_result         import LSIResult

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Полный результат пайплайна."""
    lsi:         LSIResult
    signals:     Dict[str, pd.DataFrame] = field(default_factory=dict)  # module_name → DataFrame
    raw_data:    Dict[str, Any]          = field(default_factory=dict)
    computed_at: str                     = ""


class Pipeline:

    def __init__(self, cache_dir: str = "./cache"):
        self.cbr_fetcher      = CBRFetcher(cache_dir=f"{cache_dir}/cbr")
        self.minfin_fetcher   = MinfinFetcher(cache_dir=f"{cache_dir}/minfin")
        self.fns_fetcher      = FNSFetcher(cache_dir=f"{cache_dir}/fns")
        self.roskazna_fetcher = RoskaznaFetcher(cache_dir=f"{cache_dir}/cbr")
        self.cache            = ParquetCache()
        self.modules = [
            M1Reserves(),
            M2Repo(),
            M3OFZ(),
            M4Tax(),
            M5Treasury(),
        ]
        self.engine = LSIEngine()

    def execute(self) -> LSIResult:
        return self.execute_full().lsi

    def execute_full(self) -> PipelineResult:
        logger.info("Pipeline: старт")
        data       = self._fetch_all()
        signals_dfs = self._compute_signals(data)
        lsi        = self.engine.compute(signals_dfs)
        logger.info("Pipeline: LSI=%.3f [%s]", lsi.value, lsi.status)
        return PipelineResult(
            lsi=lsi,
            signals=signals_dfs,
            raw_data=data,
            computed_at=datetime.now().isoformat(timespec="seconds"),
        )

    def _fetch_all(self) -> dict:
        data = {}
        cbr = self.cbr_fetcher.fetch()
        if cbr.status in ("success", "partial") and cbr.data:
            data.update(cbr.data)

        # Полные данные репо со спросом и cover_ratio
        try:
            repo_full = pd.read_csv(
                f"{self.cbr_fetcher.cache_dir}/repo_full.csv",
                parse_dates=["date"]
            )
            data["repo_full"] = repo_full
        except Exception:
            data["repo_full"] = pd.DataFrame()

        minfin = self.minfin_fetcher.fetch()
        if minfin.data:
            data["ofz"] = minfin.data.get("ofz")

        fns = self.fns_fetcher.fetch()
        if fns.data:
            data["tax_calendar"] = fns.data.get("tax_calendar")
            data["target_date"]  = datetime.now()

        if "bliquidity" not in data or data["bliquidity"] is None:
            rk = self.roskazna_fetcher.fetch()
            if rk.data:
                data["bliquidity"] = rk.data.get("bliquidity")

        return data

    def _compute_signals(self, data: dict) -> Dict[str, pd.DataFrame]:
        """Запускает каждый модуль, возвращает dict {module_name: DataFrame}."""
        result = {}
        for module in self.modules:
            try:
                df = module.compute(data)
                result[module.name] = df
                latest_flag = "нет данных" if df.empty else "ok"
                logger.info("%s: %d строк, %s", module.name, len(df), latest_flag)
            except Exception as e:
                logger.error("%s ошибка: %s", module.name, e)
                result[module.name] = pd.DataFrame()
        return result
