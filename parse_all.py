"""
Парсинг всех источников данных по ТЗ ПСБ.
Результат: pars_data/*.csv

Источники:
  M1: CBR резервы (Excel), RUONIA (HTML)
  M2: CBR репо-аукционы (HTML), параметры репо (HTML), ключевая ставка (HTML)
  M3: Минфин ОФЗ-аукционы (HTML)
  M4: Налоговый календарь ФНС (генерация по НК РФ — сайт JS-рендеринг)
  M5: CBR структурный баланс bliquidity (HTML), CBR sors Excel (HTML→Excel)
      Росказна: SSL РФ недоступен — пропускаем, фиксируем в лог
"""

import ssl
import sys
import logging
import urllib.request
import urllib.parse
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

OUT = Path("pars_data")
OUT.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("parse_all")

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LiquiditySentinel/1.0)"}
TIMEOUT = 60

results: dict[str, str] = {}   # имя → статус


# ── утилиты ──────────────────────────────────────────────────────────────────

def _save(name: str, df: pd.DataFrame) -> None:
    path = OUT / f"{name}.csv"
    df.to_csv(path, index=False)
    log.info("✓ %s  →  %s  (%d строк)", name, path, len(df))
    results[name] = f"OK  {len(df)} строк"


def _fail(name: str, reason: str) -> None:
    log.warning("✗ %s  —  %s", name, reason)
    results[name] = f"FAIL  {reason}"


def _get(url: str, params: dict | None = None, verify: bool = True) -> requests.Response:
    return requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT, verify=verify)


def _html_table(url: str, params: dict | None = None, verify: bool = True) -> pd.DataFrame | None:
    try:
        r = _get(url, params=params, verify=verify)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if not table:
            return None
        rows, header = [], []
        for i, tr in enumerate(table.find_all("tr")):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if not cells:
                continue
            if i == 0:
                header = cells
            else:
                rows.append(cells)
        if not rows:
            return None
        max_cols = max(len(r) for r in rows)
        header = header[:max_cols] if header else [f"col{i}" for i in range(max_cols)]
        rows = [r[:max_cols] + [""] * (max_cols - len(r)) for r in rows]
        return pd.DataFrame(rows, columns=header)
    except Exception as e:
        log.debug("_html_table %s: %s", url, e)
        return None


def _date_params(date_from: str = "01.01.2010") -> dict:
    return {
        "UniDbQuery.Posted": "True",
        "UniDbQuery.From": date_from,
        "UniDbQuery.To": datetime.now().strftime("%d.%m.%Y"),
    }


def _clean_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str).str.replace(r"[\s\xa0 ]", "", regex=True).str.replace(",", "."),
        errors="coerce",
    )


# ── M1: Обязательные резервы (Excel ЦБ) ─────────────────────────────────────

def parse_m1_reserves() -> None:
    url = "https://www.cbr.ru/vfs/hd_base/RReserves/required_reserves_table.xlsx"
    try:
        r = _get(url)
        r.raise_for_status()
        raw = BytesIO(r.content)

        # Читаем без заголовка — строка 2 содержит имена колонок, строка 3+ данные
        df_raw = pd.read_excel(raw, sheet_name=0, header=None, dtype=str)

        # Ищем строку с "период" или "фактическ" — это заголовок
        header_row = 2
        for i, row in df_raw.iterrows():
            vals = row.dropna().astype(str).str.lower().tolist()
            if any(kw in v for v in vals for kw in ("фактическ", "период усреднения")):
                header_row = int(i)
                break

        # Колонки берём из строки header_row, данные — со следующей
        headers = df_raw.iloc[header_row].tolist()
        data    = df_raw.iloc[header_row + 1:].reset_index(drop=True)
        data.columns = [f"c{i}" for i in range(len(data.columns))]

        # Маппинг по ключевым словам
        col_map = {}
        for i, h in enumerate(headers):
            h_low = str(h).lower()
            if any(k in h_low for k in ("период усреднения", "первый день")):
                col_map[f"c{i}"] = "date"
            elif "фактическ" in h_low and "c0" not in col_map.values():
                col_map[f"c{i}"] = "actual_avg_bln"
            elif "подлежащ" in h_low:
                col_map[f"c{i}"] = "required_avg_bln"
            elif any(k in h_low for k in ("учет", "учёт")):
                col_map[f"c{i}"] = "required_account_bln"

        data = data.rename(columns=col_map)
        keep = [c for c in ("date","actual_avg_bln","required_avg_bln","required_account_bln") if c in data.columns]
        df = data[keep].copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        for col in ("actual_avg_bln","required_avg_bln","required_account_bln"):
            if col in df.columns:
                df[col] = _clean_num(df[col])
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        _save("m1_reserves", df)
    except Exception as e:
        _fail("m1_reserves", str(e))


# ── M1: RUONIA ───────────────────────────────────────────────────────────────

def parse_m1_ruonia() -> None:
    url = "https://www.cbr.ru/hd_base/ruonia/dynamics/"
    try:
        df = _html_table(url, _date_params("01.01.2010"))
        if df is None or df.empty:
            raise ValueError("Таблица не найдена")
        df.columns = ["date", "ruonia_pct"] + list(df.columns[2:])
        df = df[["date", "ruonia_pct"]].copy()
        df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")
        df["ruonia_pct"] = _clean_num(df["ruonia_pct"])
        df = df.dropna().sort_values("date").reset_index(drop=True)
        _save("m1_ruonia", df)
    except Exception as e:
        _fail("m1_ruonia", str(e))


# ── M2: Репо-аукционы ────────────────────────────────────────────────────────

def parse_m2_repo() -> None:
    url = "https://www.cbr.ru/hd_base/repo/"
    try:
        df = _html_table(url, _date_params("01.01.2010"))
        if df is None or df.empty:
            raise ValueError("Таблица не найдена")
        df.columns = ["type","term_days","date","time","volume_mln","rate_wavg","settlement"][:len(df.columns)]
        df["date"]       = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")
        df["term_days"]  = pd.to_numeric(df["term_days"], errors="coerce")
        df["volume_mln"] = _clean_num(df["volume_mln"])
        df["volume_bln"] = df["volume_mln"] / 1000
        df["rate_wavg"]  = _clean_num(df["rate_wavg"])
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        _save("m2_repo_auctions", df)
    except Exception as e:
        _fail("m2_repo_auctions", str(e))


# ── M2: Параметры репо (лимиты) ──────────────────────────────────────────────

def parse_m2_repo_params() -> None:
    url = "https://www.cbr.ru/hd_base/dirrepoauctionparam/"
    try:
        df = _html_table(url, _date_params("01.01.2015"))
        if df is None or df.empty:
            raise ValueError("Таблица не найдена")
        df.columns = ["date","instrument_type","term_raw","settle1","settle2","limit_bln","min_rate"][:len(df.columns)]
        df["date"]      = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")
        df["term_days"] = pd.to_numeric(df["term_raw"].str.extract(r"(\d+)")[0], errors="coerce")
        df["limit_bln"] = _clean_num(df["limit_bln"])
        df["min_rate"]  = _clean_num(df["min_rate"])
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        _save("m2_repo_params", df[["date","term_days","limit_bln","min_rate"]])
    except Exception as e:
        _fail("m2_repo_params", str(e))


# ── M2: Ключевая ставка ──────────────────────────────────────────────────────

def parse_m2_keyrate() -> None:
    url = "https://www.cbr.ru/hd_base/keyrate/"
    try:
        df = _html_table(url, _date_params("01.01.2010"))
        if df is None or df.empty:
            raise ValueError("Таблица не найдена")
        df.columns = ["date", "keyrate_pct"] + list(df.columns[2:])
        df = df[["date", "keyrate_pct"]].copy()
        df["date"]        = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")
        df["keyrate_pct"] = _clean_num(df["keyrate_pct"])
        df = df.dropna().sort_values("date").reset_index(drop=True)
        _save("m2_keyrate", df)
    except Exception as e:
        _fail("m2_keyrate", str(e))


# ── M3: Аукционы ОФЗ Минфин ─────────────────────────────────────────────────

def parse_m3_ofz() -> None:
    url = "https://minfin.gov.ru/ru/document/?id_4=315131"
    try:
        df = _html_table(url, verify=False)
        if df is None or df.empty:
            raise ValueError("Таблица не найдена")

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
                rename[c] = "offer_volume_bln"
            elif "спрос" in cl:
                rename[c] = "demand_volume_bln"
            elif "размещен" in cl or "выручк" in cl:
                rename[c] = "placement_volume_bln"
            elif "доходн" in cl:
                rename[c] = "avg_yield_pct"
        df = df.rename(columns=rename)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
        for col in ["offer_volume_bln","demand_volume_bln","placement_volume_bln","avg_yield_pct"]:
            if col in df.columns:
                df[col] = _clean_num(df[col])

        import numpy as np
        if "demand_volume_bln" in df.columns and "placement_volume_bln" in df.columns:
            df["cover_ratio"] = df["demand_volume_bln"] / df["placement_volume_bln"].replace(0, np.nan)
        if "demand_volume_bln" in df.columns and "offer_volume_bln" in df.columns:
            df["bid_cover"] = df["demand_volume_bln"] / df["offer_volume_bln"].replace(0, np.nan)

        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        _save("m3_ofz_auctions", df)
    except Exception as e:
        _fail("m3_ofz_auctions", str(e))


# ── M4: Налоговый календарь ФНС (генерация по НК РФ) ────────────────────────

HOLIDAYS_RAW = {
    2014:[date(2014,1,1),date(2014,1,2),date(2014,1,3),date(2014,1,6),date(2014,1,7),date(2014,1,8),date(2014,2,24),date(2014,3,10),date(2014,5,1),date(2014,5,2),date(2014,5,9),date(2014,6,12),date(2014,6,13),date(2014,11,4)],
    2015:[date(2015,1,1),date(2015,1,2),date(2015,1,8),date(2015,1,9),date(2015,2,23),date(2015,3,9),date(2015,5,1),date(2015,5,11),date(2015,6,12),date(2015,11,4)],
    2016:[date(2016,1,1),date(2016,1,7),date(2016,1,8),date(2016,2,22),date(2016,2,23),date(2016,3,7),date(2016,3,8),date(2016,5,2),date(2016,5,3),date(2016,5,9),date(2016,6,13),date(2016,11,4)],
    2017:[date(2017,1,2),date(2017,1,3),date(2017,1,6),date(2017,1,9),date(2017,2,23),date(2017,2,24),date(2017,3,8),date(2017,5,1),date(2017,5,8),date(2017,5,9),date(2017,6,12),date(2017,11,6)],
    2018:[date(2018,1,1),date(2018,1,2),date(2018,1,3),date(2018,1,5),date(2018,1,8),date(2018,2,23),date(2018,3,8),date(2018,3,9),date(2018,4,30),date(2018,5,1),date(2018,5,2),date(2018,5,9),date(2018,6,11),date(2018,6,12),date(2018,11,5),date(2018,12,31)],
    2019:[date(2019,1,1),date(2019,1,2),date(2019,1,3),date(2019,1,4),date(2019,1,7),date(2019,1,8),date(2019,3,8),date(2019,5,1),date(2019,5,3),date(2019,5,9),date(2019,5,10),date(2019,6,12),date(2019,11,4)],
    2020:[date(2020,1,1),date(2020,1,2),date(2020,1,3),date(2020,1,6),date(2020,1,7),date(2020,1,8),date(2020,2,24),date(2020,3,9),date(2020,5,1),date(2020,5,4),date(2020,5,5),date(2020,6,12),date(2020,11,4)],
    2021:[date(2021,1,1),date(2021,1,4),date(2021,1,5),date(2021,1,6),date(2021,1,7),date(2021,1,8),date(2021,2,22),date(2021,2,23),date(2021,3,8),date(2021,5,3),date(2021,5,10),date(2021,6,14),date(2021,11,4),date(2021,11,5),date(2021,12,31)],
    2022:[date(2022,1,3),date(2022,1,4),date(2022,1,5),date(2022,1,6),date(2022,1,7),date(2022,1,10),date(2022,2,23),date(2022,3,7),date(2022,3,8),date(2022,5,2),date(2022,5,9),date(2022,5,10),date(2022,6,13),date(2022,11,4)],
    2023:[date(2023,1,2),date(2023,1,3),date(2023,1,4),date(2023,1,5),date(2023,1,6),date(2023,1,9),date(2023,2,23),date(2023,2,24),date(2023,3,8),date(2023,5,1),date(2023,5,8),date(2023,5,9),date(2023,6,12),date(2023,11,6)],
    2024:[date(2024,1,1),date(2024,1,2),date(2024,1,3),date(2024,1,4),date(2024,1,5),date(2024,1,8),date(2024,2,23),date(2024,3,8),date(2024,4,29),date(2024,4,30),date(2024,5,1),date(2024,5,9),date(2024,5,10),date(2024,6,12),date(2024,11,4),date(2024,12,31)],
    2025:[date(2025,1,1),date(2025,1,2),date(2025,1,3),date(2025,1,6),date(2025,1,7),date(2025,1,8),date(2025,2,24),date(2025,3,10),date(2025,5,1),date(2025,5,2),date(2025,5,8),date(2025,5,9),date(2025,6,12),date(2025,6,13),date(2025,11,3),date(2025,11,4),date(2025,12,31)],
    2026:[date(2026,1,1),date(2026,1,2),date(2026,1,5),date(2026,1,6),date(2026,1,7),date(2026,1,8),date(2026,1,9),date(2026,2,23),date(2026,3,9),date(2026,5,1),date(2026,5,11),date(2026,6,12)],
    2027:[date(2027,1,1),date(2027,1,4),date(2027,1,5),date(2027,1,6),date(2027,1,7),date(2027,1,8),date(2027,2,22),date(2027,2,23),date(2027,3,8),date(2027,5,3),date(2027,5,10),date(2027,6,14),date(2027,11,4)],
}
ALL_HOLIDAYS = {d for days in HOLIDAYS_RAW.values() for d in days}


def _next_biz(d: date) -> date:
    while d.weekday() >= 5 or d in ALL_HOLIDAYS:
        d += timedelta(days=1)
    return d


def parse_m4_tax_calendar() -> None:
    rows = []
    for year in range(2014, datetime.now().year + 2):
        enp = year >= 2023
        for month in range(1, 13):
            if enp:
                d = _next_biz(date(year, month, 28))
                rows.append({"date": d, "tax_type": "ЕНП", "description": f"Единый налоговый платёж {month:02d}/{year}"})
                if month in [1,4,7,10]:
                    rows.append({"date": d, "tax_type": "НДС (квартал)", "description": f"НДС {month:02d}/{year}"})
                if month in [3,6,9,12]:
                    rows.append({"date": d, "tax_type": "Налог на прибыль", "description": f"Прибыль {month:02d}/{year}"})
            else:
                if month in [1,4,7,10]:
                    d = _next_biz(date(year, month, 25))
                    rows.append({"date": d, "tax_type": "НДС (квартал)", "description": f"НДС {month:02d}/{year}"})
                d = _next_biz(date(year, month, 15))
                rows.append({"date": d, "tax_type": "Страховые взносы", "description": f"Взносы {month:02d}/{year}"})
                if month in [3,6,9,12]:
                    d = _next_biz(date(year, month, 28))
                    rows.append({"date": d, "tax_type": "Налог на прибыль", "description": f"Прибыль {month:02d}/{year}"})
                d = _next_biz(date(year, month, 25))
                rows.append({"date": d, "tax_type": "Акцизы", "description": f"Акцизы {month:02d}/{year}"})
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    _save("m4_tax_calendar", df)


# ── M5: Структурный баланс ликвидности (ЦБ bliquidity) ──────────────────────

def parse_m5_bliquidity() -> None:
    url = "https://www.cbr.ru/hd_base/bliquidity/"
    params = _date_params("01.01.2014")
    qs = urllib.parse.urlencode(params)
    full_url = f"{url}?{qs}"
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(full_url, context=ctx, timeout=TIMEOUT) as resp:
            html = resp.read().decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            raise ValueError("Таблица не найдена")
        rows = []
        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) >= 2:
                rows.append({"date": cells[0], "structural_balance_bln": cells[1]})
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")
        df["structural_balance_bln"] = _clean_num(df["structural_balance_bln"])
        df = df.dropna().sort_values("date").reset_index(drop=True)
        _save("m5_bliquidity", df)
    except Exception as e:
        _fail("m5_bliquidity", str(e))


# ── M5: CBR sors — привлечённые средства банков (Excel) ─────────────────────

def parse_m5_sors() -> None:
    url = "https://www.cbr.ru/statistics/bank_sector/sors/"
    try:
        r = _get(url)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Ищем ссылки на Excel-файлы
        excel_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(ext in href.lower() for ext in (".xlsx", ".xls", ".csv")):
                if not href.startswith("http"):
                    href = "https://www.cbr.ru" + href
                excel_links.append(href)

        if not excel_links:
            raise ValueError("Excel-ссылки на странице sors не найдены")

        dfs = []
        for link in excel_links[:3]:  # берём первые 3 файла
            try:
                resp = _get(link)
                resp.raise_for_status()
                df_part = pd.read_excel(BytesIO(resp.content), sheet_name=0)
                dfs.append(df_part)
                log.info("  sors: скачан %s (%d строк)", link.split("/")[-1], len(df_part))
            except Exception as ex:
                log.debug("  sors skip %s: %s", link, ex)

        if not dfs:
            raise ValueError("Ни один Excel-файл sors не загружен")

        df = pd.concat(dfs, ignore_index=True)
        _save("m5_sors_raw", df)
    except Exception as e:
        _fail("m5_sors", str(e))


# ── M5: Росказна ─────────────────────────────────────────────────────────────

def parse_m5_roskazna() -> None:
    _fail("m5_roskazna", "SSL РФ недоступен (российский корневой сертификат); данные не получены")


# ── запуск всех парсеров ─────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=== Парсинг источников ТЗ ПСБ → pars_data/ ===\n")

    parse_m1_reserves()
    parse_m1_ruonia()
    parse_m2_repo()
    parse_m2_repo_params()
    parse_m2_keyrate()
    parse_m3_ofz()
    parse_m4_tax_calendar()
    parse_m5_bliquidity()
    parse_m5_sors()
    parse_m5_roskazna()

    log.info("\n=== Итог ===")
    ok = [k for k, v in results.items() if v.startswith("OK")]
    fail = [k for k, v in results.items() if v.startswith("FAIL")]
    for k, v in results.items():
        log.info("  %-30s %s", k, v)
    log.info("\nУспешно: %d / %d", len(ok), len(results))
    if fail:
        log.warning("Не удалось: %s", ", ".join(fail))
        sys.exit(1)
