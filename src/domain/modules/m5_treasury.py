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

import numpy as np
import pandas as pd

from .base import BaseModule
from ..normalization.mad import mad_normalize

MAD_WINDOW      = 260
DRAIN_THRESHOLD = 500.0

TZ_COLUMNS = ["date", "MAD_score_ЦБ", "MAD_score_Росказна", "Flag_Budget_Drain"]


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

    # ── внутренние вычисления ───────────────────────────────────────────────

    def _calculate(self, bliq_df: pd.DataFrame) -> pd.DataFrame:
        df = bliq_df.copy().sort_values("date").reset_index(drop=True)

        bal_col = "structural_balance_bln" if "structural_balance_bln" in df.columns else "balance"
        if bal_col not in df.columns:
            return pd.DataFrame()

        df["balance"]      = pd.to_numeric(df[bal_col], errors="coerce")
        df["weekly_delta"] = df["balance"].diff(periods=5)

        df["MAD_score_ЦБ"] = mad_normalize(df["balance"], window=MAD_WINDOW)

        # MAD_score_Росказна: proxy — оттоки казначейства (delta > 0 = баланс растёт = банки теряют ликвидность)
        roskazna_flows         = df["weekly_delta"].clip(lower=0)
        df["MAD_score_Росказна"] = mad_normalize(roskazna_flows, window=MAD_WINDOW)

        # Flag_Budget_Drain: только пики оттока (локальные максимумы > порога),
        # не весь период превышения. distance=5 → один пик на эпизод.
        from scipy.signal import find_peaks
        delta_vals = df["weekly_delta"].fillna(0).values
        peaks, _   = find_peaks(delta_vals, height=DRAIN_THRESHOLD, distance=5)
        df["Flag_Budget_Drain"] = 0
        df.loc[peaks, "Flag_Budget_Drain"] = 1
        return df
