# application/pipeline.py
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any

import pandas as pd
from sqlalchemy.orm import Session

from src.infrastructure.fetchers.cbr import CBRFetcher
from src.infrastructure.fetchers.minfin import MinfinFetcher
from src.infrastructure.fetchers.fns import FNSFetcher
from src.infrastructure.fetchers.roskazna import RoskaznaFetcher
from src.infrastructure.fetchers.cached_fetcher import CachedFetcher
from src.domain.modules.m1_reserves import M1Reserves
from src.domain.modules.m2_repo import M2Repo
from src.domain.modules.m3_ofz import M3OFZ
from src.domain.modules.m4_tax import M4Tax
from src.domain.modules.m5_treasury import M5Treasury
from src.domain.aggregation.lsi_engine import LSIEngine
from src.domain.models.lsi_result import LSIResult

logger = logging.getLogger(__name__)

# TTL в часах для каждого источника
_TTL = {
    "cbr_main":      6,    # резервы, репо, RUONIA — ежедневно после 18:00
    "minfin_ofz":    12,   # аукционы ОФЗ — по средам
    "fns_tax":       168,  # налоговый календарь — раз в год
    "roskazna":      24,   # ЕКС — ежемесячно
}


@dataclass
class PipelineResult:
    lsi:         LSIResult
    signals:     Dict[str, pd.DataFrame] = field(default_factory=dict)
    raw_data:    Dict[str, Any] = field(default_factory=dict)
    computed_at: str = ""


class Pipeline:

    def __init__(self, session: Session, force_refresh: bool = False):
        self.session = session
        self.force_refresh = force_refresh

        # Оборачиваем каждый фетчер в CachedFetcher
        self._cbr = CachedFetcher(
            fetcher=CBRFetcher(),
            source_key="cbr_main",
            ttl_hours=_TTL["cbr_main"],
            session=session,
            force_refresh=force_refresh,
        )
        self._minfin = CachedFetcher(
            fetcher=MinfinFetcher(),
            source_key="minfin_ofz",
            ttl_hours=_TTL["minfin_ofz"],
            session=session,
            force_refresh=force_refresh,
        )
        self._fns = CachedFetcher(
            fetcher=FNSFetcher(),
            source_key="fns_tax",
            ttl_hours=_TTL["fns_tax"],
            session=session,
            force_refresh=force_refresh,
        )
        self._roskazna = CachedFetcher(
            fetcher=RoskaznaFetcher(),
            source_key="roskazna",
            ttl_hours=_TTL["roskazna"],
            session=session,
            force_refresh=force_refresh,
        )

        self.modules = [M1Reserves(), M2Repo(), M3OFZ(), M4Tax(), M5Treasury()]
        self.engine = LSIEngine()

    def execute(self) -> LSIResult:
        return self.execute_full().lsi

    def execute_full(self) -> PipelineResult:
        logger.info("Pipeline: старт")
        data = self._fetch_all()
        signals_dfs = self._compute_signals(data)
        lsi = self.engine.compute(signals_dfs)
        logger.info("Pipeline: LSI=%.3f [%s]", lsi.value, lsi.status)
        return PipelineResult(
            lsi=lsi,
            signals=signals_dfs,
            raw_data=data,
            computed_at=datetime.now().isoformat(timespec="seconds"),
        )

    def _fetch_all(self) -> dict:
        data = {}

        # CBR — может вернуть несколько DataFrame внутри data{}
        cbr = self._cbr.fetch()
        if cbr.status in ("success", "cached", "stale") and cbr.data is not None:
            if isinstance(cbr.data, dict):
                data.update(cbr.data)
            else:
                # CachedFetcher вернул DataFrame напрямую
                data["cbr"] = cbr.data
            logger.info("CBR: статус=%s", cbr.status)

        # Minfin OFZ
        minfin = self._minfin.fetch()
        if minfin.data is not None:
            if isinstance(minfin.data, dict):
                data["ofz"] = minfin.data.get("ofz")
            else:
                data["ofz"] = minfin.data
            logger.info("Minfin OFZ: статус=%s", minfin.status)

        # FNS налоговый календарь
        fns = self._fns.fetch()
        if fns.data is not None:
            if isinstance(fns.data, dict):
                data["tax_calendar"] = fns.data.get("tax_calendar")
            else:
                data["tax_calendar"] = fns.data
            data["target_date"] = datetime.now()
            logger.info("FNS: статус=%s", fns.status)

        # Росказна — только если bliquidity ещё не пришёл из CBR
        if "bliquidity" not in data or data["bliquidity"] is None:
            rk = self._roskazna.fetch()
            if rk.data is not None:
                if isinstance(rk.data, dict):
                    data["bliquidity"] = rk.data.get("bliquidity")
                else:
                    data["bliquidity"] = rk.data
            logger.info("Росказна: статус=%s", rk.status)

        # Логируем что получили
        for key, val in data.items():
            if isinstance(val, pd.DataFrame):
                logger.info("data['%s']: %d строк", key, len(val))

        return data

    def _compute_signals(self, data: dict) -> Dict[str, pd.DataFrame]:
        result = {}
        for module in self.modules:
            try:
                df = module.compute(data)
                result[module.name] = df
                logger.info(
                    "%s: %d строк, %s",
                    module.name, len(df),
                    "нет данных" if df.empty else "ok"
                )
            except Exception as e:
                logger.error("%s ошибка: %s", module.name, e)
                result[module.name] = pd.DataFrame()
        return result
