"""
М5 — Средства федерального казначейства / баланс ликвидности

Выход по ТЗ (pd.DataFrame):
  date               — дата
  MAD_score_ЦБ       — MAD(structural_balance, окно 260), SNR=0.92
  MAD_score_Росказна — MAD(оттоки казначейства, proxy через delta > 0)
  Flag_Budget_Drain  — 1 если баланс растёт > 500 млрд/нед (отток из банков)

Конвенция ЦБ РФ:
  balance > 0 → дефицит банков (занимают у ЦБ)  → стресс → высокий MAD_score_ЦБ
  balance < 0 → профицит банков (размещают в ЦБ) → норма
"""

from typing import Dict, Any

import pandas as pd

from src.domain.modules.base import BaseModule
from src.domain.normalization.mad import mad_normalize

MAD_WINDOW = 260
WEEKLY_WINDOW = 156
DRAIN_THRESHOLD = 500.0

TZ_COLUMNS = ["date", "MAD_score_ЦБ", "MAD_score_Росказна", "Flag_Budget_Drain",
              "MAD_score_депозиты", "Flag_Proficit"]


class M5Treasury(BaseModule):

    def __init__(self):
        super().__init__(name="M5_TREASURY", weight=0.167)

    def compute(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        data["bliquidity"] — DataFrame: date, structural_balance_bln

        Returns:
            DataFrame с колонками по ТЗ.
        """
        bliq_df = data.get("bliquidity", pd.DataFrame())
        if bliq_df is None or (isinstance(bliq_df, pd.DataFrame) and bliq_df.empty):
            return pd.DataFrame(columns=TZ_COLUMNS)

        df = self._calculate(bliq_df)
        if df.empty:
            return pd.DataFrame(columns=TZ_COLUMNS)

        return df[TZ_COLUMNS].copy()

    def _calculate(self, bliq_df: pd.DataFrame) -> pd.DataFrame:
        df = bliq_df.copy().sort_values("date").reset_index(drop=True)
        df["date"] = pd.to_datetime(df["date"])

        bal_col = next(
            (c for c in ["structural_balance_bln", "balance"] if c in df.columns), None)
        if bal_col is None:
            return pd.DataFrame()

        df["balance"] = pd.to_numeric(df[bal_col], errors="coerce")

        df["MAD_score_ЦБ"] = mad_normalize(df["balance"], window=MAD_WINDOW)

        df_w = (
            df.set_index("date")["balance"]
            .resample("W").last()
            .dropna()
            .reset_index()
        )
        df_w.columns = ["date", "balance"]
        df_w["weekly_delta"] = df_w["balance"].diff(1)
        df_w["MAD_score_Росказна"] = mad_normalize(
            df_w["weekly_delta"], window=WEEKLY_WINDOW)
        df_w["_week"] = df_w["date"].dt.to_period("W")
        from scipy.signal import find_peaks
        delta_vals = df_w["weekly_delta"].fillna(0).values
        peaks, _ = find_peaks(delta_vals, height=DRAIN_THRESHOLD, distance=8)

        drain_dates = set()
        for idx in peaks:
            peak_sunday = df_w.loc[idx, "date"]
            week_start = peak_sunday - pd.Timedelta(days=6)
            week_days = df[(df["date"] >= week_start) & (
                df["date"] <= peak_sunday)]["date"]
            if not week_days.empty:
                drain_dates.add(week_days.iloc[-1])

        df["_week"] = df["date"].dt.to_period("W")
        df_w["_week"] = df_w["date"].dt.to_period("W")
        df = df.merge(df_w[["_week", "MAD_score_Росказна"]],
                      on="_week", how="left")
        df["Flag_Budget_Drain"] = df["date"].isin(drain_dates).astype(int)
        df.drop(columns=["_week"], inplace=True)
        deposits_proxy = df["balance"].clip(upper=0).abs()
        df["MAD_score_депозиты"] = mad_normalize(
            deposits_proxy, window=MAD_WINDOW)

        df["Flag_Proficit"] = (df["balance"] < -500).astype(int)

        return df
