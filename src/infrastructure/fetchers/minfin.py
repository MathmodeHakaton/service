"""
Fetcher для данных Минфина: аукционы ОФЗ.

Улучшен относительно заглушки: реализован полный парсинг
на основе liquidity_sentinel/modules/m3_ofz.py.

Ключевое исправление cover_ratio:
  Правильно: demand / placement (спрос / размещение) — показывает интерес рынка
  Неправильно: demand / offer — offer это лимит, не рыночный сигнал
"""

import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from .base import BaseFetcher, FetcherResult

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LiquiditySentinel/1.0)"}
MINFIN_OFZ_URL = "https://minfin.gov.ru/ru/document/?id_4=315131"


class MinfinFetcher(BaseFetcher):
    """Fetcher для аукционов ОФЗ Минфина с полным парсингом."""

    def __init__(self, cache_dir: str = "./cache/minfin", timeout: int = 60, retries: int = 3):
        super().__init__(timeout=timeout, retries=retries)
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch(self) -> FetcherResult:
        """Получить результаты аукционов ОФЗ."""
        try:
            df = self.fetch_ofz()
            return FetcherResult(
                data={"ofz": df},
                last_updated=datetime.now(),
                status="success",
                source_url=MINFIN_OFZ_URL,
            )
        except Exception as e:
            logger.warning("Minfin fetch failed: %s", e)
            return FetcherResult(
                data={"ofz": pd.DataFrame()},
                last_updated=datetime.now(),
                status="error",
                error_message=str(e),
                source_url=MINFIN_OFZ_URL,
            )

    def fetch_ofz(self) -> pd.DataFrame:
        """
        Парсит аукционы ОФЗ с сайта Минфина.

        cover_ratio = demand / placement (правильная формула по российской практике).
        bid_cover   = demand / offer (объёмный индикатор; норма < 1.0 для РФ).
        """
        cache = self.cache_dir / "ofz_auctions.csv"
        try:
            r = requests.get(MINFIN_OFZ_URL, headers=HEADERS,
                             timeout=self.timeout, verify=False)
            r.raise_for_status()
            df = self._parse_minfin_html(r.text)
            if df is not None and len(df) > 0:
                df.to_csv(cache, index=False)
                logger.info("Minfin OFZ: %d аукционов", len(df))
                return df
        except Exception as e:
            logger.warning("Minfin сайт недоступен: %s", e)

        if cache.exists():
            df = pd.read_csv(cache, parse_dates=["date"])
            logger.info("Minfin OFZ из кэша: %d записей", len(df))
            return df

        raise RuntimeError("Нет данных ОФЗ Минфина")

    def _parse_minfin_html(self, html: str) -> pd.DataFrame | None:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            return None

        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        rows = []
        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if cells:
                rows.append(cells)

        if not rows:
            return None

        df = pd.DataFrame(
            rows, columns=headers[:len(rows[0])] if headers else None)
        return self._normalize_columns(df)

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Стандартизация колонок Минфина."""
        rename = {}
        for c in df.columns:
            cl = c.lower()
            if "дата" in cl:
                rename[c] = "date"
            elif "формат" in cl or "вид" in cl:
                rename[c] = "auction_format"
            elif "выпуск" in cl or "серия" in cl:
                rename[c] = "issue"
            elif "предложен" in cl:
                rename[c] = "offer_volume"
            elif "спрос" in cl:
                rename[c] = "demand_volume"
            elif "размещен" in cl or "выручк" in cl:
                rename[c] = "placement_volume"
            elif "доходн" in cl and "средн" in cl:
                rename[c] = "avg_yield"

        df = df.rename(columns=rename)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(
                df["date"], dayfirst=True, errors="coerce")

        for col in ["offer_volume", "demand_volume", "placement_volume", "avg_yield"]:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str)
                    .str.replace(r"[\s\xa0]", "", regex=True)
                    .str.replace(",", "."),
                    errors="coerce",
                )

        # cover_ratio = demand / placement (правильная формула)
        if "demand_volume" in df.columns and "placement_volume" in df.columns:
            df["cover_ratio"] = df["demand_volume"] / \
                df["placement_volume"].replace(0, np.nan)

        # bid_cover = demand / offer (объёмный, норма < 1 для РФ)
        if "demand_volume" in df.columns and "offer_volume" in df.columns:
            df["bid_cover"] = df["demand_volume"] / \
                df["offer_volume"].replace(0, np.nan)

        return df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
