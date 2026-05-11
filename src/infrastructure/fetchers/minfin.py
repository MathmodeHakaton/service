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
                # Получаем все строки
                all_rows = table.find_all("tr")
                if len(all_rows) < 2:
                    logger.debug(f"Таблица {idx}: слишком мало строк")
                    continue

                # Ищем заголовки в th или первой строке td
                header_row = all_rows[0]
                headers = [th.get_text(strip=True)
                           for th in header_row.find_all("th")]

                if not headers:
                    headers = [td.get_text(strip=True)
                               for td in header_row.find_all("td")]

                if not headers:
                    continue

                logger.debug(
                    f"Таблица {idx} заголовки ({len(headers)}): {headers[:3]}...")

                # Проверяем наличие ключевых колонок (должна быть дата или размещение)
                headers_lower = [h.lower() for h in headers]
                has_date = any(
                    "дата" in h or "date" in h for h in headers_lower)
                has_placement = any(
                    "размещен" in h or "placement" in h for h in headers_lower)

                if not (has_date or has_placement):
                    continue

                # Парсим строки данных
                rows = []
                for tr in all_rows[1:]:
                    cells = [td.get_text(strip=True)
                             for td in tr.find_all(["td", "th"])]
                    if not cells:
                        continue
                    # Фильтруем пустые строки
                    if all(not c or c.isspace() for c in cells):
                        continue
                    rows.append(cells)

                if not rows:
                    logger.debug(f"Таблица {idx}: нет строк данных")
                    continue

                logger.info(
                    f"Таблица {idx}: найдено {len(rows)} строк, {len(headers)} колонок")

                # Обработаем дублирующиеся имена колонок
                headers = self._deduplicate_headers(headers)

                # Создаём DataFrame, выравнивая количество колонок
                max_cols = max(len(row) for row in rows)
                headers = headers[:max_cols]
                while len(headers) < max_cols:
                    headers.append(f"col_{len(headers)}")

                df = pd.DataFrame(rows, columns=headers)
                logger.debug(
                    f"DataFrame создан: {df.shape}, колонки: {df.columns.tolist()}")

                normalized_df = self._normalize_columns(df)
                if normalized_df is not None and len(normalized_df) > 0:
                    logger.info(
                        f"Таблица {idx} успешно обработана, {len(normalized_df)} строк")
                    return normalized_df
            except Exception as e:
                logger.debug(
                    f"Ошибка при парсинге таблицы {idx}: {e}", exc_info=True)
                continue

        logger.warning("Ни одна таблица не подошла под критерии")
        return None

    def _deduplicate_headers(self, headers: list) -> list:
        """Обработать дублирующиеся имена колонок"""
        seen = {}
        result = []
        for h in headers:
            if h in seen:
                seen[h] += 1
                result.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                result.append(h)
        return result

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame | None:
        """Стандартизация колонок Минфина."""
        if df.empty:
            logger.warning("DataFrame пуст")
            return None

        # Обработаем дублирующиеся имена колонок, если они остались
        if df.columns.duplicated().any():
            logger.warning(
                f"Обнаружены дублирующиеся имена колонок: {df.columns.tolist()}")
            df.columns = self._deduplicate_headers(df.columns.tolist())

        logger.debug(f"Исходные колонки: {df.columns.tolist()}")

        # Первый проход: переименование колонок
        rename = {}
        for c in df.columns:
            if not isinstance(c, str):
                continue
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
                # Сначала убедимся, что это Series, а не DataFrame
                date_col = df["date"]
                if isinstance(date_col, pd.DataFrame):
                    logger.warning("date — DataFrame, беру первую колонку")
                    date_col = date_col.iloc[:, 0]

                date_col = date_col.astype(str).str.strip()
                df["date"] = pd.to_datetime(
                    date_col, dayfirst=True, errors="coerce")
                valid_dates = df["date"].notna().sum()
                logger.debug(
                    f"Дата спарсена, валидных значений: {valid_dates}/{len(df)}")

                if valid_dates == 0:
                    logger.warning("Ни одна дата не спарсена правильно")
                    return None
            except Exception as e:
                logger.warning(f"Ошибка парсинга даты: {e}")
                return None

        # Обработка числовых колонок
        numeric_cols = ["offer_volume", "demand_volume",
                        "placement_volume", "avg_yield"]
        for col in numeric_cols:
            if col not in df.columns:
                continue
            try:
                # Убедимся, что это Series
                col_data = df[col]
                if isinstance(col_data, pd.DataFrame):
                    logger.warning(f"{col} — DataFrame, беру первую колонку")
                    col_data = col_data.iloc[:, 0]

                # Конвертируем в строки и очищаем
                col_str = col_data.astype(str).str.strip()
                col_str = col_str.str.replace(r"[\s\xa0]", "", regex=True)
                col_str = col_str.str.replace(",", ".")

                df[col] = pd.to_numeric(col_str, errors="coerce")
                logger.debug(
                    f"{col}: {df[col].notna().sum()} валидных значений")
            except Exception as e:
                logger.warning(f"Ошибка конвертации колонки {col}: {e}")
                df[col] = None

        # Вычисление производных колонок
        if "demand_volume" in df.columns and "placement_volume" in df.columns:
            try:
                demand = pd.to_numeric(df["demand_volume"], errors="coerce")
                placement = pd.to_numeric(
                    df["placement_volume"], errors="coerce")
                df["cover_ratio"] = demand / placement.replace(0, np.nan)
            except Exception as e:
                logger.warning(f"Ошибка вычисления cover_ratio: {e}")

        if "demand_volume" in df.columns and "offer_volume" in df.columns:
            try:
                demand = pd.to_numeric(df["demand_volume"], errors="coerce")
                offer = pd.to_numeric(df["offer_volume"], errors="coerce")
                df["bid_cover"] = demand / offer.replace(0, np.nan)
            except Exception as e:
                logger.warning(f"Ошибка вычисления bid_cover: {e}")

        # Фильтруем по наличию даты
        if "date" in df.columns:
            initial_len = len(df)
            df = df.dropna(subset=["date"])
            logger.debug(f"После фильтра по датам: {len(df)}/{initial_len}")

            if len(df) == 0:
                logger.warning(
                    "После фильтра по датам остался пустой DataFrame")
                return None

            df = df.sort_values("date").reset_index(drop=True)
            logger.info(
                f"Нормализовано {len(df)} строк с датами от {df['date'].min()} до {df['date'].max()}")

        return df
