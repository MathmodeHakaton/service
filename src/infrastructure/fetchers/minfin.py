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
import time

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from .base import BaseFetcher, FetcherResult

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://minfin.gov.ru/",
}
MINFIN_OFZ_URL = "https://minfin.gov.ru/ru/document/?id_4=315131"


class MinfinFetcher(BaseFetcher):
    """Fetcher для аукционов ОФЗ Минфина с полным парсингом."""

    def __init__(self, cache_dir: str = "./cache/minfin", timeout: int = 60, retries: int = 3):
        super().__init__(timeout=timeout, retries=retries)
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

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
            # Небольшая задержка перед запросом
            time.sleep(0.5)

            r = self.session.get(
                MINFIN_OFZ_URL, timeout=self.timeout, verify=False)
            r.raise_for_status()
            r.encoding = 'utf-8'  # Явно указываем кодировку

            logger.debug(
                f"Minfin response status: {r.status_code}, content-length: {len(r.text)}")

            df = self._parse_minfin_html(r.text)
            if df is not None and len(df) > 0:
                df.to_csv(cache, index=False)
                logger.info("Minfin OFZ: %d аукционов", len(df))
                return df
            else:
                logger.warning("Minfin: таблица не найдена или пуста в ответе")
        except Exception as e:
            logger.warning("Minfin сайт недоступен: %s", e)

        if cache.exists():
            df = pd.read_csv(cache, parse_dates=["date"])
            logger.info("Minfin OFZ из кэша: %d записей", len(df))
            return df

        raise RuntimeError("Нет данных ОФЗ Минфина")

    def _parse_minfin_html(self, html: str) -> pd.DataFrame | None:
        soup = BeautifulSoup(html, "html.parser")

        # Ищем таблицу по различным селекторам
        tables = soup.find_all("table")
        logger.debug(f"Найдено {len(tables)} таблиц на странице")

        if not tables:
            logger.warning("Таблицы не найдены на странице")
            return None

        # Перебираем все таблицы и ищем ту, которая содержит нужные данные
        for idx, table in enumerate(tables):
            logger.debug(f"Проверяю таблицу {idx}")
            try:
                headers = [th.get_text(strip=True)
                           for th in table.find_all("th")]

                # Проверяем, содержит ли заголовок нужные слова (дата, размещение и т.д.)
                if not headers:
                    headers = [td.get_text(strip=True) for td in table.find_all("tr")[
                        0].find_all("td")]

                # первые 3 для отладки
                logger.debug(f"Таблица {idx} заголовки: {headers[:3]}")

                # Проверяем наличие ключевых колонок
                headers_lower = [h.lower() for h in headers]
                if not any("дата" in h or "date" in h for h in headers_lower):
                    continue

                rows = []
                for tr in table.find_all("tr")[1:]:
                    cells = [td.get_text(strip=True)
                             for td in tr.find_all("td")]
                    if cells and len(cells) > 2:  # Должно быть минимум несколько колонок
                        rows.append(cells)

                if rows:
                    logger.info(f"Таблица {idx}: найдено {len(rows)} строк")
                    df = pd.DataFrame(
                        rows, columns=headers[:len(rows[0])] if headers else None)
                    normalized_df = self._normalize_columns(df)
                    if normalized_df is not None and len(normalized_df) > 0:
                        return normalized_df
            except Exception as e:
                logger.debug(f"Ошибка при парсинге таблицы {idx}: {e}")
                continue

        logger.warning("Ни одна таблица не подошла под критерии")
        return None

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Стандартизация колонок Минфина."""
        if df.empty:
            return df

        logger.debug(f"Исходные колонки: {df.columns.tolist()}")

        rename = {}
        for c in df.columns:
            cl = c.lower()
            if "дата" in cl or "date" in cl:
                rename[c] = "date"
            elif "формат" in cl or "вид" in cl:
                rename[c] = "auction_format"
            elif "выпуск" in cl or "серия" in cl:
                rename[c] = "issue"
            elif "предложен" in cl or "offer" in cl:
                rename[c] = "offer_volume"
            elif "спрос" in cl or "demand" in cl:
                rename[c] = "demand_volume"
            elif "размещен" in cl or "placement" in cl or "выручк" in cl:
                rename[c] = "placement_volume"
            elif "доходн" in cl and "средн" in cl:
                rename[c] = "avg_yield"

        df = df.rename(columns=rename)
        logger.debug(f"После переименования: {df.columns.tolist()}")

        # Обработка даты
        if "date" in df.columns:
            try:
                df["date"] = pd.to_datetime(
                    df["date"], dayfirst=True, errors="coerce")
                logger.debug(
                    f"Дата спарсена, НЕ-null значений: {df['date'].notna().sum()}")
            except Exception as e:
                logger.warning(f"Ошибка парсинга даты: {e}")

        # Обработка числовых колонок
        for col in ["offer_volume", "demand_volume", "placement_volume", "avg_yield"]:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(
                        df[col].astype(str)
                        .str.replace(r"[\s\xa0]", "", regex=True)
                        .str.replace(",", "."),
                        errors="coerce",
                    )
                except Exception as e:
                    logger.warning(f"Ошибка конвертации колонки {col}: {e}")

        # cover_ratio = demand / placement (правильная формула)
        if "demand_volume" in df.columns and "placement_volume" in df.columns:
            try:
                df["cover_ratio"] = df["demand_volume"] / \
                    df["placement_volume"].replace(0, np.nan)
            except Exception as e:
                logger.warning(f"Ошибка вычисления cover_ratio: {e}")

        # bid_cover = demand / offer (объёмный, норма < 1 для РФ)
        if "demand_volume" in df.columns and "offer_volume" in df.columns:
            try:
                df["bid_cover"] = df["demand_volume"] / \
                    df["offer_volume"].replace(0, np.nan)
            except Exception as e:
                logger.warning(f"Ошибка вычисления bid_cover: {e}")

        # Фильтруем по наличию даты
        if "date" in df.columns:
            df = df.dropna(subset=["date"])

        if len(df) > 0:
            df = df.sort_values("date").reset_index(drop=True)
            logger.info(f"Нормализовано {len(df)} строк")

        return df
