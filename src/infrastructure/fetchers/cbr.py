"""
Fetcher для данных ЦБ РФ: резервы, RUONIA, репо, ключевая ставка, баланс ликвидности.

Улучшен относительно заглушки: реализован полный HTML/Excel-парсинг
на основе работающих методов из liquidity_sentinel/modules/m1_reserves.py
и liquidity_sentinel/modules/m2_repo.py.
"""

import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from .base import BaseFetcher, FetcherResult

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LiquiditySentinel/1.0)"}

RESERVES_URL   = "https://www.cbr.ru/vfs/hd_base/RReserves/required_reserves_table.xlsx"
RUONIA_URL     = "https://www.cbr.ru/hd_base/ruonia/dynamics/"
REPO_URL       = "https://www.cbr.ru/hd_base/repo/"
REPO_PARAM_URL = "https://www.cbr.ru/hd_base/dirrepoauctionparam/"
KEYRATE_URL    = "https://www.cbr.ru/hd_base/keyrate/"
BLIQ_URL       = "https://www.cbr.ru/hd_base/bliquidity/"


def _get_html_table(url: str, params: dict) -> Optional[pd.DataFrame]:
    """Универсальный загрузчик HTML-таблицы ЦБ."""
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=60)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if table is None:
            raise ValueError(f"Таблица не найдена: {url}")
        headers_row = table.find("tr")
        col_names = [th.get_text(strip=True) for th in headers_row.find_all(["th", "td"])]
        rows = []
        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if cells:
                rows.append(cells)
        if not rows:
            return None
        return pd.DataFrame(rows, columns=col_names[:len(rows[0])] if rows else col_names)
    except Exception as e:
        logger.warning("CBR HTML fetch failed %s: %s", url, e)
        return None


def _date_range_params(date_from: str = "01.01.2010") -> dict:
    return {
        "UniDbQuery.Posted": "True",
        "UniDbQuery.From": date_from,
        "UniDbQuery.To": datetime.now().strftime("%d.%m.%Y"),
    }


class CBRFetcher(BaseFetcher):
    """Fetcher для всех данных ЦБ РФ с полным парсингом."""

    def __init__(self, cache_dir: str = "./cache/cbr", timeout: int = 60, retries: int = 3):
        super().__init__(timeout=timeout, retries=retries)
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    # ── Публичный метод ────────────────────────────────────────────────────
    def fetch(self) -> FetcherResult:
        """Получить все данные ЦБ: резервы, RUONIA, репо, ключевую ставку, баланс."""
        data = {}
        errors = []

        for key, method in [
            ("reserves",   self.fetch_reserves),
            ("ruonia",     self.fetch_ruonia),
            ("repo",       self.fetch_repo),
            ("repo_params",self.fetch_repo_params),
            ("keyrate",    self.fetch_keyrate),
            ("bliquidity", self.fetch_bliquidity),
        ]:
            try:
                data[key] = method()
                logger.info("CBR[%s]: %d строк", key, len(data[key]))
            except Exception as e:
                logger.warning("CBR[%s] ошибка: %s", key, e)
                data[key] = pd.DataFrame()
                errors.append(f"{key}: {e}")

        status = "success" if not errors else ("partial" if data else "error")
        return FetcherResult(
            data=data,
            last_updated=datetime.now(),
            status=status,
            error_message="; ".join(errors) if errors else None,
        )

    # ── М1: Обязательные резервы ──────────────────────────────────────────
    def fetch_reserves(self) -> pd.DataFrame:
        """Скачивает Excel с обязательными резервами."""
        cache = self.cache_dir / "required_reserves.xlsx"
        raw = None
        try:
            r = requests.get(RESERVES_URL, headers=HEADERS, timeout=self.timeout)
            r.raise_for_status()
            raw = r.content
            cache.write_bytes(raw)
        except Exception as e:
            logger.warning("Резервы: %s, беру кэш", e)
            if cache.exists():
                raw = cache.read_bytes()
            else:
                raise

        src = BytesIO(raw)
        probe = pd.read_excel(src, sheet_name=0, header=None, nrows=15, dtype=str)
        header_row = self._find_header_row(probe)
        src.seek(0)
        df = pd.read_excel(src, sheet_name=0, header=header_row)
        df = df.dropna(how="all").reset_index(drop=True)
        return self._normalize_reserves(df)

    def _find_header_row(self, probe: pd.DataFrame) -> int:
        for i, row in probe.iterrows():
            vals = row.dropna().astype(str).str.lower().tolist()
            if any(kw in v for v in vals for kw in ("дата", "период", "date", "начало")):
                return int(i)
        return 3

    def _normalize_reserves(self, df: pd.DataFrame) -> pd.DataFrame:
        TARGET = {
            "date":             ["период усреднения", "дата", "date", "начало"],
            "actual_avg":       ["фактическ"],
            "required_avg":     ["подлежащ"],
            "required_account": ["учета", "учёта"],
        }
        assigned, col_map = set(), {}
        for target, kws in TARGET.items():
            for c in df.columns:
                if c in assigned:
                    continue
                if any(kw in str(c).lower() for kw in kws):
                    col_map[c] = target
                    assigned.add(c)
                    break
        df = df.rename(columns=col_map)
        keep = [c for c in ("date", "actual_avg", "required_avg", "required_account") if c in df.columns]
        df = df[keep].copy()
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
            df = df.dropna(subset=["date"])
        for col in ("actual_avg", "required_avg", "required_account"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_values("date").reset_index(drop=True)

    # ── М1: RUONIA ────────────────────────────────────────────────────────
    def fetch_ruonia(self, date_from: str = "01.01.2014") -> pd.DataFrame:
        """Скачивает RUONIA (HTML-парсинг)."""
        cache = self.cache_dir / "ruonia.csv"
        try:
            params = {**_date_range_params(date_from)}
            r = requests.get(RUONIA_URL, params={
                "UniDbQuery.Posted": "True",
                "UniDbQuery.From": date_from,
                "UniDbQuery.To": datetime.now().strftime("%d.%m.%Y"),
            }, headers=HEADERS, timeout=self.timeout)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table")
            if not table:
                raise ValueError("Таблица RUONIA не найдена")
            rows = []
            for tr in table.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                if len(cells) >= 2:
                    rows.append({"date": cells[0], "ruonia": cells[1]})
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")
            df["ruonia"] = pd.to_numeric(
                df["ruonia"].str.replace(",", ".").str.replace("\xa0", ""), errors="coerce"
            )
            df = df.dropna().sort_values("date").reset_index(drop=True)
            df.to_csv(cache, index=False)
            return df
        except Exception as e:
            logger.warning("RUONIA: %s, беру кэш", e)
            if cache.exists():
                return pd.read_csv(cache, parse_dates=["date"])
            raise

    # ── М2: Репо ──────────────────────────────────────────────────────────
    def fetch_repo(self, date_from: str = "21.11.2002") -> pd.DataFrame:
        """Скачивает итоги аукционов репо (все сроки, с 2002)."""
        cache = self.cache_dir / "repo_results.csv"
        params = {
            "UniDbQuery.Posted": "True",
            "UniDbQuery.From":   date_from,
            "UniDbQuery.To":     datetime.now().strftime("%d.%m.%Y"),
            "UniDbQuery.P1":     "0",
        }
        df = _get_html_table(REPO_URL, params)
        if df is not None:
            df.columns = ["type", "term_days", "date", "time", "volume_mln", "rate_wavg", "settlement"]
            df["date"]       = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")
            df["term_days"]  = pd.to_numeric(df["term_days"], errors="coerce").astype("Int64")
            df["volume_mln"] = pd.to_numeric(
                df["volume_mln"].str.replace(r"[\s\xa0]", "", regex=True).str.replace(",", "."),
                errors="coerce"
            )
            df["volume_bln"] = df["volume_mln"] / 1000
            df["rate_wavg"]  = pd.to_numeric(df["rate_wavg"].str.replace(",", "."), errors="coerce")
            df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
            df.to_csv(cache, index=False)
            return df
        if cache.exists():
            return pd.read_csv(cache, parse_dates=["date"])
        raise RuntimeError("Нет данных репо")

    def fetch_repo_params(self, date_from: str = "01.01.2015") -> pd.DataFrame:
        """Скачивает параметры аукционов репо (лимиты)."""
        cache = self.cache_dir / "repo_params.csv"
        df = _get_html_table(REPO_PARAM_URL, _date_range_params(date_from))
        if df is not None:
            df.columns = ["date", "instrument_type", "term_raw", "settle1", "settle2", "limit_bln", "min_rate"]
            df["date"]      = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")
            df["term_days"] = pd.to_numeric(df["term_raw"].str.extract(r"(\d+)")[0], errors="coerce").astype("Int64")
            df["limit_bln"] = pd.to_numeric(
                df["limit_bln"].str.replace(r"[\s\xa0]", "", regex=True).str.replace(",", "."),
                errors="coerce"
            )
            df["min_rate"]  = pd.to_numeric(df["min_rate"].str.replace(",", "."), errors="coerce")
            df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
            df.to_csv(cache, index=False)
            return df[["date", "term_days", "limit_bln", "min_rate"]]
        if cache.exists():
            return pd.read_csv(cache, parse_dates=["date"])
        return pd.DataFrame(columns=["date", "term_days", "limit_bln", "min_rate"])

    # ── М2: Ключевая ставка ───────────────────────────────────────────────
    def fetch_keyrate(self, date_from: str = "01.01.2010") -> pd.DataFrame:
        """Скачивает историю ключевой ставки."""
        cache = self.cache_dir / "keyrate.csv"
        df = _get_html_table(KEYRATE_URL, _date_range_params(date_from))
        if df is not None:
            df.columns = ["date", "keyrate"]
            df["date"]    = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")
            df["keyrate"] = pd.to_numeric(df["keyrate"].str.replace(",", "."), errors="coerce")
            df = df.dropna().sort_values("date").reset_index(drop=True)
            df.to_csv(cache, index=False)
            return df
        if cache.exists():
            return pd.read_csv(cache, parse_dates=["date"])
        raise RuntimeError("Нет данных ключевой ставки")

    # ── М5: Баланс ликвидности (все 15 колонок по ТЗ) ────────────────────
    def fetch_bliquidity(self, date_from: str = "01.02.2014") -> pd.DataFrame:
        """
        Скачивает таблицу дефицита/профицита ликвидности со всеми колонками по ТЗ:
          col2  structural_balance_bln        — дефицит/профицит ⭐⭐ ground truth
          col5  auction_repo_bln              — аукционное репо ⭐ M2
          col8  standing_secured_credit_bln   — экстренное кредитование ⭐ M2 стресс
          col14 corr_accounts_bln             — корсчета ⭐⭐ M1 факт, M5
          col15 required_reserves_bln         — норматив резервов ⭐⭐ M1
        """
        cache = self.cache_dir / "bliquidity.csv"
        COLS = [
            "date",
            "structural_balance_bln",
            "balance_ex_budget_bln",
            "loans_total_bln",
            "auction_repo_bln",
            "auction_secured_credit_bln",
            "standing_repo_bln",
            "standing_secured_credit_bln",
            "deposits_total_bln",
            "auction_deposits_bln",
            "standing_deposits_bln",
            "cobr_bln",
            "other_ops_bln",
            "corr_accounts_bln",
            "required_reserves_bln",
        ]

        def _clean(s):
            return pd.to_numeric(
                str(s).replace("\xa0", "").replace(" ", "").replace(",", "."),
                errors="coerce"
            )

        try:
            import ssl, urllib.request, urllib.parse
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            params = urllib.parse.urlencode({
                "UniDbQuery.Posted": "True",
                "UniDbQuery.From": date_from,
                "UniDbQuery.To": datetime.now().strftime("%d.%m.%Y"),
            })
            url = f"{BLIQ_URL}?{params}"
            with urllib.request.urlopen(url, context=ctx, timeout=self.timeout) as resp:
                html = resp.read().decode("utf-8")
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table")
            if not table:
                raise ValueError("Таблица bliquidity не найдена")

            records = []
            for tr in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                if len(cells) >= 15 and cells[0] not in ("1", "Дата", ""):
                    records.append(cells[:15])

            df = pd.DataFrame(records, columns=COLS)
            df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")
            for col in COLS[1:]:
                df[col] = df[col].apply(_clean)
            df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
            df.to_csv(cache, index=False)
            logger.info("bliquidity: %d строк, %d колонок", len(df), len(df.columns))
            return df
        except Exception as e:
            logger.warning("bliquidity: %s, беру кэш", e)
            if cache.exists():
                return pd.read_csv(cache, parse_dates=["date"])
            raise
