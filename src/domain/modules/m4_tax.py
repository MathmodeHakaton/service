"""
М4 — Налоговый период и сезонность

Выход по ТЗ (pd.DataFrame):
  date                — дата
  Tax_Week_Flag       — 1 в ±7 дней от квартального налогового события
  End_of_Month_Flag   — 1 в последние 3 дня месяца
  End_of_Quarter_Flag — 1 в последние 3 дня квартала
  Seasonal_Factor     — мультипликатор 1.0–1.4 для LSI
"""

from datetime import datetime
from typing import Dict, Any

import pandas as pd

from src.domain.modules.base import BaseModule

TAX_WEEK_WINDOW = 7
EOM_WINDOW = 3
SF_EOQ = 1.4
SF_EOM = 1.2
SF_TAX_WEEK = 1.1
SF_NORMAL = 1.0

QUARTERLY_TYPES = {"НДС (квартал)", "Налог на прибыль"}

TZ_COLUMNS = ["date", "Tax_Week_Flag", "End_of_Month_Flag",
              "End_of_Quarter_Flag", "Seasonal_Factor"]


class M4Tax(BaseModule):

    def __init__(self):
        super().__init__(name="M4_TAX", weight=0.0)

    def compute(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        data["tax_calendar"] — DataFrame: date, tax_type
        data["target_date"]  — опорная дата (default: сегодня)

        Если передан только target_date — считает флаги на один день.
        Для backtest используй compute_series().

        Returns:
            DataFrame с колонками по ТЗ (одна строка для target_date).
        """
        tax_df = data.get("tax_calendar", pd.DataFrame())
        target_date = pd.Timestamp(data.get("target_date", datetime.now()))

        if tax_df.empty:
            return pd.DataFrame(columns=TZ_COLUMNS)

        tax_df = tax_df.copy()
        tax_df["date"] = pd.to_datetime(tax_df["date"])

        row = self._flags_for_date(target_date, tax_df)
        return pd.DataFrame([{"date": target_date, **row}])[TZ_COLUMNS]

    def compute_series(self, dates: pd.Series, tax_df: pd.DataFrame) -> pd.DataFrame:
        """
        Вычислить флаги для ряда дат (для backtest и дашборда).

        Returns:
            DataFrame с колонками по ТЗ для каждой даты.
        """
        tax_df = tax_df.copy()
        tax_df["date"] = pd.to_datetime(tax_df["date"])
        quarterly = tax_df[tax_df["tax_type"].isin(QUARTERLY_TYPES)]
        q_dates = quarterly["date"].tolist()

        target = pd.to_datetime(dates).sort_values().reset_index(drop=True)
        result = pd.DataFrame({"date": target})

        result["Tax_Week_Flag"] = result["date"].apply(
            lambda d: int(any(abs((d - kd).days) <=
                          TAX_WEEK_WINDOW for kd in q_dates))
        )
        result["End_of_Month_Flag"] = result["date"].apply(
            lambda d: int((d + pd.offsets.MonthEnd(0) - d).days < EOM_WINDOW)
        )
        result["End_of_Quarter_Flag"] = result["date"].apply(
            lambda d: int(d.month in [3, 6, 9, 12] and
                          (d + pd.offsets.MonthEnd(0) - d).days < EOM_WINDOW)
        )
        result["Seasonal_Factor"] = result.apply(self._seasonal_factor, axis=1)
        return result[TZ_COLUMNS]

    def _flags_for_date(self, d: pd.Timestamp, tax_df: pd.DataFrame) -> dict:
        quarterly = tax_df[tax_df["tax_type"].isin(QUARTERLY_TYPES)]
        q_dates = quarterly["date"].tolist()

        tw = int(any(abs((d - kd).days) <= TAX_WEEK_WINDOW for kd in q_dates))
        eom = int((d + pd.offsets.MonthEnd(0) - d).days < EOM_WINDOW)
        eoq = int(d.month in [3, 6, 9, 12] and (
            d + pd.offsets.MonthEnd(0) - d).days < EOM_WINDOW)

        if eoq:
            sf = SF_EOQ
        elif eom:
            sf = SF_EOM
        elif tw:
            sf = SF_TAX_WEEK
        else:
            sf = SF_NORMAL

        return {
            "Tax_Week_Flag":       tw,
            "End_of_Month_Flag":   eom,
            "End_of_Quarter_Flag": eoq,
            "Seasonal_Factor":     sf,
        }

    def _seasonal_factor(self, row) -> float:
        if row["End_of_Quarter_Flag"]:
            return SF_EOQ
        if row["End_of_Month_Flag"]:
            return SF_EOM
        if row["Tax_Week_Flag"]:
            return SF_TAX_WEEK
        return SF_NORMAL
