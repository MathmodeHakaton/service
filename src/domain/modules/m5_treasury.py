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
WEEKLY_WINDOW   = 156     # 3 года в неделях (по ТЗ: скользящее окно 3 года)
DRAIN_THRESHOLD = 500.0   # пик оттока > 500 млрд/нед (ТЗ: 300–500 млрд)

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

    # ── внутренние вычисления ───────────────────────────────────────────────

    def _calculate(self, bliq_df: pd.DataFrame) -> pd.DataFrame:
        df = bliq_df.copy().sort_values("date").reset_index(drop=True)
        df["date"] = pd.to_datetime(df["date"])

        bal_col = next((c for c in ["structural_balance_bln", "balance"] if c in df.columns), None)
        if bal_col is None:
            return pd.DataFrame()

        df["balance"] = pd.to_numeric(df[bal_col], errors="coerce")

        # MAD_score_ЦБ: уровень структурного баланса на дневных данных
        df["MAD_score_ЦБ"] = mad_normalize(df["balance"], window=MAD_WINDOW)

        # MAD_score_Росказна: proxy через еженедельную дельту баланса.
        # Проблема старого кода: diff(5) на дневных данных + clip(0) → медиана≈0 →
        # MAD≈0 → z-score взрывается. Фикс: ресемплируем к неделям, берём diff(1),
        # не клипируем — тогда медиана ≠ 0, MAD осмысленный.
        # MAD_score_Росказна: используем corr_accounts (col14) если доступен,
        # иначе fallback на дельту structural_balance.
        # corr_accounts = «живые деньги» банков на корсчетах — прямой индикатор
        # бюджетного канала (падает при налоговых платежах, растёт при расходах бюджета).
        if "corr_accounts_bln" in df.columns:
            corr_series = df.set_index("date")["corr_accounts_bln"]
        else:
            corr_series = df.set_index("date")["balance"]

        df_w = (
            corr_series
            .resample("W").last()
            .dropna()
            .reset_index()
        )
        df_w.columns = ["date", "value"]
        df_w["weekly_delta"] = df_w["value"].diff(1)
        # + = отток с корсчетов (банки теряют ликвидность, стресс)
        # − = приток (бюджетные расходы пришли в систему, норма)
        df_w["MAD_score_Росказна"] = mad_normalize(df_w["weekly_delta"], window=WEEKLY_WINDOW)
        df_w["_week"] = df_w["date"].dt.to_period("W")

        # Flag_Budget_Drain: пики оттока > 500 млрд/нед (ТЗ: 300–500).
        # distance=8 → минимум 2 месяца между пиками (исключает кластеры внутри квартала).
        from scipy.signal import find_peaks
        delta_vals = df_w["weekly_delta"].fillna(0).values
        peaks, _   = find_peaks(delta_vals, height=DRAIN_THRESHOLD, distance=8)

        # Для каждого пика находим последний рабочий день той же недели в дневном ряду.
        # Флаг ставим на ОДИН день (не на всю неделю) — иначе 5 маркеров на графике.
        drain_dates = set()
        for idx in peaks:
            peak_sunday = df_w.loc[idx, "date"]
            week_start  = peak_sunday - pd.Timedelta(days=6)
            week_days   = df[(df["date"] >= week_start) & (df["date"] <= peak_sunday)]["date"]
            if not week_days.empty:
                drain_dates.add(week_days.iloc[-1])

        # Мержим MAD_score_Росказна на дневной индекс по периоду недели
        df["_week"] = df["date"].dt.to_period("W")
        df_w["_week"] = df_w["date"].dt.to_period("W")
        df = df.merge(df_w[["_week", "MAD_score_Росказна"]], on="_week", how="left")
        df["Flag_Budget_Drain"] = df["date"].isin(drain_dates).astype(int)
        df.drop(columns=["_week"], inplace=True)

        # col10 + col11: депозиты банков в ЦБ — контр-сигнал профицита.
        # Высокий уровень = у банков избыток денег, стресса нет.
        # MAD_score_депозиты: большой положительный → профицит, отрицательный → дефицит.
        dep_cols = [c for c in ["auction_deposits_bln", "standing_deposits_bln"] if c in df.columns]
        if dep_cols:
            df["total_deposits_bln"]   = df[dep_cols].fillna(0).sum(axis=1)
            df["MAD_score_депозиты"]   = mad_normalize(df["total_deposits_bln"], window=MAD_WINDOW)
            # Flag_Proficit: устойчивый профицит (депозиты > 1000 млрд — банки паркуют много)
            df["Flag_Proficit"] = (df["total_deposits_bln"] > 1000).astype(int)
        else:
            df["MAD_score_депозиты"] = np.nan
            df["Flag_Proficit"]      = 0

        return df
