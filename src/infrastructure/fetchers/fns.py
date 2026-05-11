"""
Fetcher для налогового календаря ФНС.

Сайт nalog.gov.ru использует JS-рендеринг — парсинг невозможен.
Данные генерируются программно по правилам НК РФ (та же логика,
что в liquidity_sentinel/modules/m4_tax.py).

Структура ДО 2023: НДС 25-е, Взносы 15-е, Акцизы 25-е
Структура С 2023:  ЕНП 28-е (объединяет большинство налогов)
"""

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

import pandas as pd

from src.infrastructure.fetchers.base import BaseFetcher, FetcherResult

logger = logging.getLogger(__name__)

ENP_START_YEAR = 2023

HOLIDAYS: dict = {
    2014: [date(2014, 1, 1), date(2014, 1, 2), date(2014, 1, 3), date(2014, 1, 6), date(2014, 1, 7),
           date(2014, 1, 8), date(2014, 2, 24), date(
               2014, 3, 10), date(2014, 5, 1), date(2014, 5, 2),
           date(2014, 5, 9), date(2014, 6, 12), date(2014, 6, 13), date(2014, 11, 4)],
    2015: [date(2015, 1, 1), date(2015, 1, 2), date(2015, 1, 8), date(2015, 1, 9), date(2015, 2, 23),
           date(2015, 3, 9), date(2015, 5, 1), date(2015, 5, 11), date(2015, 6, 12), date(2015, 11, 4)],
    2016: [date(2016, 1, 1), date(2016, 1, 7), date(2016, 1, 8), date(2016, 2, 22), date(2016, 2, 23),
           date(2016, 3, 7), date(2016, 3, 8), date(
               2016, 5, 2), date(2016, 5, 3), date(2016, 5, 9),
           date(2016, 6, 13), date(2016, 11, 4)],
    2017: [date(2017, 1, 2), date(2017, 1, 3), date(2017, 1, 6), date(2017, 1, 9), date(2017, 2, 23),
           date(2017, 2, 24), date(2017, 3, 8), date(
               2017, 5, 1), date(2017, 5, 8), date(2017, 5, 9),
           date(2017, 6, 12), date(2017, 11, 6)],
    2018: [date(2018, 1, 1), date(2018, 1, 2), date(2018, 1, 3), date(2018, 1, 5), date(2018, 1, 8),
           date(2018, 2, 23), date(2018, 3, 8), date(
               2018, 3, 9), date(2018, 4, 30), date(2018, 5, 1),
           date(2018, 5, 2), date(2018, 5, 9), date(
               2018, 6, 11), date(2018, 6, 12),
           date(2018, 11, 5), date(2018, 12, 31)],
    2019: [date(2019, 1, 1), date(2019, 1, 2), date(2019, 1, 3), date(2019, 1, 4), date(2019, 1, 7),
           date(2019, 1, 8), date(2019, 3, 8), date(
               2019, 5, 1), date(2019, 5, 3), date(2019, 5, 9),
           date(2019, 5, 10), date(2019, 6, 12), date(2019, 11, 4)],
    2020: [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 6), date(2020, 1, 7),
           date(2020, 1, 8), date(2020, 2, 24), date(
               2020, 3, 9), date(2020, 5, 1), date(2020, 5, 4),
           date(2020, 5, 5), date(2020, 6, 12), date(2020, 11, 4)],
    2021: [date(2021, 1, 1), date(2021, 1, 4), date(2021, 1, 5), date(2021, 1, 6), date(2021, 1, 7),
           date(2021, 1, 8), date(2021, 2, 22), date(
               2021, 2, 23), date(2021, 3, 8), date(2021, 5, 3),
           date(2021, 5, 10), date(2021, 6, 14), date(2021, 11, 4), date(2021, 11, 5), date(2021, 12, 31)],
    2022: [date(2022, 1, 3), date(2022, 1, 4), date(2022, 1, 5), date(2022, 1, 6), date(2022, 1, 7),
           date(2022, 1, 10), date(2022, 2, 23), date(
               2022, 3, 7), date(2022, 3, 8), date(2022, 5, 2),
           date(2022, 5, 9), date(2022, 5, 10), date(2022, 6, 13), date(2022, 11, 4)],
    2023: [date(2023, 1, 2), date(2023, 1, 3), date(2023, 1, 4), date(2023, 1, 5), date(2023, 1, 6),
           date(2023, 1, 9), date(2023, 2, 23), date(
               2023, 2, 24), date(2023, 3, 8), date(2023, 5, 1),
           date(2023, 5, 8), date(2023, 5, 9), date(2023, 6, 12), date(2023, 11, 6)],
    2024: [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5),
           date(2024, 1, 8), date(2024, 2, 23), date(
               2024, 3, 8), date(2024, 4, 29), date(2024, 4, 30),
           date(2024, 5, 1), date(2024, 5, 9), date(
               2024, 5, 10), date(2024, 6, 12),
           date(2024, 11, 4), date(2024, 12, 31)],
    2025: [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 6), date(2025, 1, 7),
           date(2025, 1, 8), date(2025, 2, 24), date(
               2025, 3, 10), date(2025, 5, 1), date(2025, 5, 2),
           date(2025, 5, 8), date(2025, 5, 9), date(
               2025, 6, 12), date(2025, 6, 13),
           date(2025, 11, 3), date(2025, 11, 4), date(2025, 12, 31)],
    2026: [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7),
           date(2026, 1, 8), date(2026, 1, 9), date(
               2026, 2, 23), date(2026, 3, 9),
           date(2026, 5, 1), date(2026, 5, 11), date(2026, 6, 12)],
    2027: [date(2027, 1, 1), date(2027, 1, 4), date(2027, 1, 5), date(2027, 1, 6), date(2027, 1, 7),
           date(2027, 1, 8), date(2027, 2, 22), date(
               2027, 2, 23), date(2027, 3, 8),
           date(2027, 5, 3), date(2027, 5, 10), date(2027, 6, 14), date(2027, 11, 4)],
}
ALL_HOLIDAYS = {d for days in HOLIDAYS.values() for d in days}


def _next_business_day(d: date) -> date:
    while d.weekday() >= 5 or d in ALL_HOLIDAYS:
        d += timedelta(days=1)
    return d


class FNSFetcher(BaseFetcher):
    """
    Генератор налогового календаря по НК РФ.
    Сайт ФНС не парсится (JS), поэтому генерируем программно.
    """

    def __init__(self, cache_dir: str = "./cache/fns", timeout: int = 30, retries: int = 1):
        super().__init__(timeout=timeout, retries=retries)
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch(self) -> FetcherResult:
        """Сгенерировать налоговый календарь."""
        try:
            df = self.generate_tax_calendar()
            return FetcherResult(
                data={"tax_calendar": df},
                last_updated=datetime.now(),
                status="success",
                source_url=None,
            )
        except Exception as e:
            return FetcherResult(
                data={"tax_calendar": pd.DataFrame()},
                last_updated=datetime.now(),
                status="error",
                error_message=str(e),
                source_url=None,
            )

    def generate_tax_calendar(self, start_year: int = 2014, end_year: int = None) -> pd.DataFrame:
        """Генерирует даты налоговых событий по правилам НК РФ."""
        if end_year is None:
            end_year = datetime.now().year + 1
        years = list(range(start_year, end_year + 1))
        events = self._generate_events(years)
        df = pd.DataFrame(events)
        df["date"] = pd.to_datetime(df["date"])
        cache = self.cache_dir / "tax_calendar.csv"
        df.to_csv(cache, index=False)
        logger.info("FNS: сгенерировано %d налоговых событий (%d-%d)",
                    len(df), start_year, end_year)
        return df

    def _generate_events(self, years: List[int]) -> List[dict]:
        events = []
        for year in years:
            use_enp = year >= ENP_START_YEAR
            for month in range(1, 13):
                if use_enp:
                    enp = _next_business_day(date(year, month, 28))
                    events.append({"date": enp, "tax_type": "ЕНП",
                                   "description": f"Единый налоговый платёж {month:02d}/{year}"})
                    if month in [1, 4, 7, 10]:
                        nds = _next_business_day(date(year, month, 28))
                        events.append({"date": nds, "tax_type": "НДС (квартал)",
                                       "description": f"НДС квартальный {month:02d}/{year}"})
                    if month in [3, 6, 9, 12]:
                        profit = _next_business_day(date(year, month, 28))
                        events.append({"date": profit, "tax_type": "Налог на прибыль",
                                       "description": f"Налог на прибыль {month:02d}/{year}"})
                else:
                    if month in [1, 4, 7, 10]:
                        nds = _next_business_day(date(year, month, 25))
                        events.append({"date": nds, "tax_type": "НДС (квартал)",
                                       "description": f"НДС квартальный {month:02d}/{year}"})
                    vzn = _next_business_day(date(year, month, 15))
                    events.append({"date": vzn, "tax_type": "Страховые взносы",
                                   "description": f"Страховые взносы {month:02d}/{year}"})
                    if month in [3, 6, 9, 12]:
                        profit = _next_business_day(date(year, month, 28))
                        events.append({"date": profit, "tax_type": "Налог на прибыль",
                                       "description": f"Налог на прибыль {month:02d}/{year}"})
                acc = _next_business_day(date(year, month, 25))
                events.append({"date": acc, "tax_type": "Акцизы",
                               "description": f"Акцизы {month:02d}/{year}"})
        return events
