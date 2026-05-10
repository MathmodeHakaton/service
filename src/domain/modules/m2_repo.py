"""
М2 — Аукционы репо ЦБ (7-дневные)

Выход по ТЗ (pd.DataFrame):
  date                  — дата аукциона
  MAD_score_cover       — MAD(cover ratio, окно 30)
  MAD_score_rate_spread — MAD(ставка − ключевая, окно 30), SNR=1.34
  Flag_Demand           — cover > 2.0 (proxy: utilization > 0.9 или MAD > 3.5)
"""

from typing import Dict, Any

import numpy as np
import pandas as pd

from .base import BaseModule
from ..normalization.mad import mad_normalize

PRIMARY_TERM          = 7
MAD_WINDOW            = 30
FLAG_DEMAND_THRESHOLD = 3.5
FLAG_COVER_THRESHOLD  = 0.9

TZ_COLUMNS = ["date", "MAD_score_cover", "MAD_score_rate_spread", "Flag_Demand"]


class M2Repo(BaseModule):

    def __init__(self):
        super().__init__(name="M2_REPO", weight=0.333)

    def compute(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        data["repo"]        — DataFrame: date, term_days, volume_bln, rate_wavg
        data["keyrate"]     — DataFrame: date, keyrate
        data["repo_params"] — DataFrame: date, term_days, limit_bln  (опционально)

        Returns:
            DataFrame с колонками по ТЗ.
        """
        repo_df    = data.get("repo",        pd.DataFrame())
        keyrate_df = data.get("keyrate",     pd.DataFrame())
        params_df  = data.get("repo_params", pd.DataFrame())

        if repo_df.empty or keyrate_df.empty:
            return pd.DataFrame(columns=TZ_COLUMNS)

        df = self._calculate(repo_df, keyrate_df, params_df)
        if df.empty:
            return pd.DataFrame(columns=TZ_COLUMNS)

        return df[TZ_COLUMNS].copy()

    # ── внутренние вычисления ───────────────────────────────────────────────

    def _calculate(self, repo_df, keyrate_df, params_df) -> pd.DataFrame:
        df = repo_df[repo_df["term_days"] == PRIMARY_TERM].copy()
        df = df.sort_values("date").reset_index(drop=True)

        kr = keyrate_df.sort_values("date").set_index("date")["keyrate"]
        df["keyrate"]     = df["date"].map(lambda d: kr.asof(d) if d >= kr.index.min() else np.nan)
        df["rate_spread"] = df["rate_wavg"] - df["keyrate"]

        if not params_df.empty:
            p7 = params_df[params_df["term_days"] == PRIMARY_TERM][["date", "limit_bln"]]
            df = df.merge(p7, on="date", how="left")
            df["utilization"] = np.where(
                df["limit_bln"] > 0, df["volume_bln"] / df["limit_bln"], np.nan
            )
        else:
            df["limit_bln"]   = np.nan
            df["utilization"] = np.nan

        df["MAD_score_rate_spread"] = mad_normalize(df["rate_spread"], window=MAD_WINDOW)
        df["MAD_score_cover"]       = mad_normalize(df["utilization"], window=MAD_WINDOW)
        # Флаг только когда окно заполнено минимум наполовину (MAD_WINDOW//2 точек)
        # Защита от ложных срабатываний при малом окне в начале ряда
        row_num = df["MAD_score_rate_spread"].notna().cumsum()
        df["Flag_Demand"] = (
            (df["MAD_score_rate_spread"].fillna(0) > FLAG_DEMAND_THRESHOLD) &
            (row_num >= MAD_WINDOW // 2)
        ).astype(int)
        return df
