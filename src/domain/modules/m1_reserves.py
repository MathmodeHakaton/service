"""
М1 — Усреднение обязательных резервов + RUONIA

Выход по ТЗ (pd.DataFrame):
  date              — дата периода усреднения
  MAD_score_спред   — MAD(rel_spread, окно 36 мес.), SNR=2.82
  MAD_score_RUONIA  — MAD(RUONIA,     окно 1000 дней), SNR=4.16
  Flag_EndOfPeriod  — 1 в последние 5 дней периода усреднения

Дополнительно (не в ТЗ):
  Flag_AboveKey     — RUONIA > ключевой 3+ дней из 5
"""

from typing import Dict, Any

import numpy as np
import pandas as pd

from .base import BaseModule
from ..normalization.mad import mad_normalize

MAD_WINDOW_MONTHLY = 36
MAD_WINDOW_DAILY   = 1000
ABOVE_KEY_DAYS     = 3
END_OF_PERIOD_DAYS = 5

TZ_COLUMNS = ["date", "MAD_score_спред", "MAD_score_RUONIA", "Flag_EndOfPeriod"]


class M1Reserves(BaseModule):

    def __init__(self):
        super().__init__(name="M1_RESERVES", weight=0.25)

    def compute(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        data["reserves"] — DataFrame: date, actual_avg, required_avg
        data["ruonia"]   — DataFrame: date, ruonia
        data["keyrate"]  — DataFrame: date, keyrate  (опционально)

        Returns:
            DataFrame с колонками по ТЗ + Flag_AboveKey.
            Пустой DataFrame если данных нет.
        """
        reserves_df = data.get("reserves", pd.DataFrame())
        ruonia_df   = data.get("ruonia",   pd.DataFrame())
        keyrate_df  = data.get("keyrate",  pd.DataFrame())

        if reserves_df.empty or ruonia_df.empty:
            return pd.DataFrame(columns=TZ_COLUMNS + ["Flag_AboveKey"])

        df = self._calculate(reserves_df, ruonia_df, keyrate_df)

        out_cols = [c for c in
                    ["date", "MAD_score_спред", "MAD_score_RUONIA",
                     "Flag_EndOfPeriod", "Flag_AboveKey"]
                    if c in df.columns]
        return df[out_cols].copy()

    # ── внутренние вычисления ───────────────────────────────────────────────

    def _calculate(self, reserves_df, ruonia_df, keyrate_df) -> pd.DataFrame:
        df = reserves_df.copy().sort_values("date").reset_index(drop=True)

        if "actual_avg" in df.columns and "required_avg" in df.columns:
            df["spread"]     = df["actual_avg"] - df["required_avg"]
            df["rel_spread"] = df["spread"] / df["required_avg"].replace(0, np.nan)
        else:
            df["spread"] = df["rel_spread"] = np.nan

        ru = ruonia_df.set_index("date")["ruonia"].sort_index()
        ru.index = pd.DatetimeIndex(ru.index)

        ruonia_m = (
            ru.resample("ME").mean().reset_index()
            .rename(columns={"ruonia": "ruonia_avg"})
        )
        ruonia_mad = (
            mad_normalize(ru, window=MAD_WINDOW_DAILY)
            .resample("ME").last().reset_index()
            .rename(columns={"ruonia": "MAD_score_RUONIA"})
        )

        flag_above = pd.Series(0, index=ru.index, dtype=int)
        if not keyrate_df.empty:
            kr       = keyrate_df.sort_values("date").set_index("date")["keyrate"]
            kr_daily = kr.reindex(ru.index, method="ffill")
            above    = (ru - kr_daily) > 0
            flag_above = (above.rolling(5, min_periods=1).sum() >= ABOVE_KEY_DAYS).astype(int)

        flag_above_m = flag_above.resample("ME").max().reset_index()
        flag_above_m.columns = ["date", "Flag_AboveKey"]

        for aux in [ruonia_m, ruonia_mad, flag_above_m]:
            aux["month_end"] = pd.to_datetime(aux["date"]) + pd.offsets.MonthEnd(0)

        df["month_end"] = df["date"] + pd.offsets.MonthEnd(0)
        df = (
            df
            .merge(ruonia_m[["month_end", "ruonia_avg"]], on="month_end", how="left")
            .merge(ruonia_mad[["month_end", "MAD_score_RUONIA"]], on="month_end", how="left")
            .merge(flag_above_m[["month_end", "Flag_AboveKey"]], on="month_end", how="left")
        )
        df["Flag_AboveKey"]    = df["Flag_AboveKey"].fillna(0).astype(int)
        df["Flag_EndOfPeriod"] = df["date"].apply(
            lambda d: int(1 <= d.day <= END_OF_PERIOD_DAYS + 1)
        )
        df = df.sort_values("date").reset_index(drop=True)
        df["MAD_score_спред"] = mad_normalize(df["rel_spread"].ffill(), window=MAD_WINDOW_MONTHLY)
        return df
