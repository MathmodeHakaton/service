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

from src.domain.modules.base import BaseModule
from src.domain.normalization.mad import mad_normalize

MAD_WINDOW_MONTHLY = 36
MAD_WINDOW_DAILY = 1000
ABOVE_KEY_DAYS = 3
END_OF_PERIOD_DAYS = 5

TZ_COLUMNS = ["date", "MAD_score_спред",
              "MAD_score_RUONIA", "Flag_EndOfPeriod"]


class M1Reserves(BaseModule):

    def __init__(self):
        super().__init__(name="M1_RESERVES", weight=0.25)

    def compute(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Приоритет источников:
          data["bliquidity"] col14 (corr_accounts) + col15 (required_reserves) — дневные ⭐
          data["reserves"]   — ежемесячный Excel ЦБ (fallback)
        data["ruonia"]   — ставка МБК
        data["keyrate"]  — ключевая ставка
        """
        ruonia_df = data.get("ruonia",  pd.DataFrame())
        keyrate_df = data.get("keyrate", pd.DataFrame())

        if ruonia_df.empty:
            return pd.DataFrame(columns=TZ_COLUMNS + ["Flag_AboveKey"])

        bliq_df = data.get("bliquidity", pd.DataFrame())
        if (bliq_df is not None and not bliq_df.empty
                and "corr_accounts_bln" in bliq_df.columns
                and "required_reserves_bln" in bliq_df.columns):
            reserves_df = self._bliq_to_reserves(bliq_df)
        else:
            reserves_df = data.get("reserves", pd.DataFrame())

        if reserves_df.empty:
            return pd.DataFrame(columns=TZ_COLUMNS + ["Flag_AboveKey"])

        df = self._calculate(reserves_df, ruonia_df, keyrate_df)

        out_cols = [c for c in
                    ["date", "MAD_score_спред", "MAD_score_RUONIA",
                     "Flag_EndOfPeriod", "Flag_AboveKey"]
                    if c in df.columns]
        return df[out_cols].copy()

    # ── конвертация bliquidity col14/15 в формат reserves ─────────────────

    @staticmethod
    def _bliq_to_reserves(bliq_df: pd.DataFrame) -> pd.DataFrame:
        """col14=corr_accounts, col15=required_reserves → actual_avg / required_avg."""
        df = bliq_df[["date", "corr_accounts_bln",
                      "required_reserves_bln"]].copy()
        df = df.rename(columns={
            "corr_accounts_bln":    "actual_avg",
            "required_reserves_bln": "required_avg",
        })
        df["date"] = pd.to_datetime(df["date"])
        return df.dropna(subset=["actual_avg", "required_avg"]).sort_values("date").reset_index(drop=True)

    # ── внутренние вычисления ───────────────────────────────────────────────

    def _calculate(self, reserves_df, ruonia_df, keyrate_df) -> pd.DataFrame:
        df = reserves_df.copy().sort_values("date").reset_index(drop=True)
        df["date"] = pd.to_datetime(df["date"])

        if "actual_avg" in df.columns and "required_avg" in df.columns:
            df["spread"] = df["actual_avg"] - df["required_avg"]
            df["rel_spread"] = df["spread"] / \
                df["required_avg"].replace(0, np.nan)
        else:
            df["spread"] = df["rel_spread"] = np.nan

        # Определяем гранулярность данных: дневные (bliquidity) или месячные (Excel)
        is_daily = len(df) > 100 and df["date"].diff().dt.days.median() < 5

        ru = ruonia_df.set_index("date")["ruonia"].sort_index()
        ru.index = pd.DatetimeIndex(ru.index)

        # MAD_score_RUONIA — всегда на дневных данных RUONIA
        mad_ruonia_daily = mad_normalize(ru, window=MAD_WINDOW_DAILY)

        # Flag_AboveKey — дневной
        flag_above = pd.Series(0, index=ru.index, dtype=int)
        if not keyrate_df.empty:
            kr = keyrate_df.sort_values("date").set_index("date")["keyrate"]
            kr_daily = kr.reindex(ru.index, method="ffill")
            above = (ru - kr_daily) > 0
            flag_above = (above.rolling(5, min_periods=1).sum()
                          >= ABOVE_KEY_DAYS).astype(int)

        if is_daily:
            # Дневные bliquidity-данные: мержим напрямую
            ru_df = ru.reset_index().rename(columns={"ruonia": "ruonia_avg"})
            ru_df["MAD_score_RUONIA"] = mad_ruonia_daily.values
            ru_df["Flag_AboveKey"] = flag_above.values
            df = pd.merge_asof(df.sort_values("date"),
                               ru_df.sort_values("date"),
                               on="date", direction="nearest", tolerance=pd.Timedelta("3d"))
            window = MAD_WINDOW_DAILY  # 260 дней ≈ 1 год
        else:
            # Ежемесячные Excel-данные: агрегируем по месяцу
            ruonia_m = ru.resample("ME").mean().reset_index().rename(
                columns={"ruonia": "ruonia_avg"})
            ruonia_mad = mad_ruonia_daily.resample("ME").last().reset_index().rename(
                columns={"ruonia": "MAD_score_RUONIA"})
            flag_m = flag_above.resample("ME").max().reset_index()
            flag_m.columns = ["date", "Flag_AboveKey"]
            for aux in [ruonia_m, ruonia_mad, flag_m]:
                aux["month_end"] = pd.to_datetime(
                    aux["date"]) + pd.offsets.MonthEnd(0)
            df["month_end"] = df["date"] + pd.offsets.MonthEnd(0)
            df = (df
                  .merge(ruonia_m[["month_end", "ruonia_avg"]],     on="month_end", how="left")
                  .merge(ruonia_mad[["month_end", "MAD_score_RUONIA"]], on="month_end", how="left")
                  .merge(flag_m[["month_end", "Flag_AboveKey"]],    on="month_end", how="left"))
            window = MAD_WINDOW_MONTHLY

        df["Flag_AboveKey"] = df.get("Flag_AboveKey", pd.Series(
            0, index=df.index)).fillna(0).astype(int)
        df["MAD_score_RUONIA"] = df.get(
            "MAD_score_RUONIA", pd.Series(np.nan, index=df.index))

        today = pd.Timestamp.now().normalize()
        df["Flag_EndOfPeriod"] = 0
        if not df.empty:
            days_left = (today + pd.offsets.MonthEnd(0) - today).days
            df.loc[df.index[-1],
                   "Flag_EndOfPeriod"] = int(days_left <= END_OF_PERIOD_DAYS)

        df = df.sort_values("date").reset_index(drop=True)
        df["MAD_score_спред"] = mad_normalize(
            df["rel_spread"].ffill(), window=window)
        return df
