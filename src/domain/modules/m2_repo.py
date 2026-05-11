"""
М2 — Аукционы репо ЦБ (7-дневные)

Выход по ТЗ (pd.DataFrame):
  date                  — дата аукциона
  MAD_score_cover       — MAD(cover ratio = спрос/размещение, окно 30)
  MAD_score_rate_spread — MAD(ставка − ключевая, окно 30), SNR=1.34
  Flag_Demand           — cover > 2.0 (спрос превысил размещение вдвое)
"""

from typing import Dict, Any

import numpy as np
import pandas as pd

from .base import BaseModule
from ..normalization.mad import mad_normalize

PRIMARY_TERM          = 7
MAD_WINDOW            = 30
FLAG_COVER_THRESHOLD  = 2.0   # ТЗ: cover > 2.0

TZ_COLUMNS = ["date", "MAD_score_cover", "MAD_score_rate_spread", "Flag_Demand"]


class M2Repo(BaseModule):

    def __init__(self):
        super().__init__(name="M2_REPO", weight=0.333)

    def compute(self, data: Dict[str, Any]) -> pd.DataFrame:
        keyrate_df = data.get("keyrate", pd.DataFrame())
        if keyrate_df.empty:
            return pd.DataFrame(columns=TZ_COLUMNS)

        # Приоритет: полные данные с cover_ratio из repo_full
        repo_full = data.get("repo_full", pd.DataFrame())
        if repo_full is not None and not repo_full.empty:
            df = self._calculate_full(repo_full, keyrate_df)
        else:
            repo_df   = data.get("repo",        pd.DataFrame())
            params_df = data.get("repo_params", pd.DataFrame())
            if repo_df.empty:
                return pd.DataFrame(columns=TZ_COLUMNS)
            df = self._calculate(repo_df, keyrate_df, params_df)

        if df.empty:
            return pd.DataFrame(columns=TZ_COLUMNS)

        # Дополняем сигналами из bliquidity col5 и col8
        bliq = data.get("bliquidity", pd.DataFrame())
        if bliq is not None and not bliq.empty:
            df = self._merge_bliquidity_signals(df, bliq)

        extra = ["MAD_score_auction_repo", "total_emergency_bln",
                 "MAD_score_emergency", "Flag_Emergency"]
        out_cols = [c for c in TZ_COLUMNS + extra if c in df.columns]
        return df[out_cols].copy()

    # ── repo_full: полные данные со спросом и cover_ratio ──────────────────

    def _calculate_full(self, repo_full: pd.DataFrame, keyrate_df: pd.DataFrame) -> pd.DataFrame:
        df = repo_full[repo_full["term_days"] == PRIMARY_TERM].copy()
        df = df.sort_values("date").reset_index(drop=True)

        kr = keyrate_df.sort_values("date").set_index("date")["keyrate"]
        df["keyrate"]     = df["date"].map(lambda d: kr.asof(d) if d >= kr.index.min() else np.nan)
        df["rate_spread"] = df["rate_wavg"] - df["keyrate"]

        df["MAD_score_rate_spread"] = mad_normalize(df["rate_spread"],  window=MAD_WINDOW)
        df["MAD_score_cover"]       = mad_normalize(df["cover_ratio"],  window=MAD_WINDOW)

        # Flag_Demand по ТЗ: cover_ratio > 2.0 (спрос вдвое превысил размещение)
        row_num = df["MAD_score_rate_spread"].notna().cumsum()
        df["Flag_Demand"] = (
            (df["cover_ratio"] > FLAG_COVER_THRESHOLD) &
            (row_num >= MAD_WINDOW // 2)
        ).astype(int)
        return df

    # ── bliquidity col5 / col8 ─────────────────────────────────────────────

    def _merge_bliquidity_signals(self, df: pd.DataFrame, bliq: pd.DataFrame) -> pd.DataFrame:
        """
        col5  auction_repo_bln              — аукционное репо ⭐ M2 основной сигнал объёма
        col7  standing_repo_bln             — репо постоянного действия (банк не смог взять на аукционе)
        col8  standing_secured_credit_bln   — обеспеченные кредиты постоянного действия ⭐ стресс
        col7+col8 = total_emergency_bln     — суммарное экстренное заимствование
        """
        want = ["date", "auction_repo_bln", "standing_repo_bln", "standing_secured_credit_bln"]
        avail = [c for c in want if c in bliq.columns]
        if "date" not in avail or len(avail) < 2:
            return df

        b = bliq[avail].copy()
        b["date"] = pd.to_datetime(b["date"])

        # col5: MAD объёма аукционного репо
        if "auction_repo_bln" in b.columns:
            b["MAD_score_auction_repo"] = mad_normalize(b["auction_repo_bln"], window=MAD_WINDOW)

        # col7 + col8: суммарное экстренное заимствование
        emergency_cols = [c for c in ["standing_repo_bln", "standing_secured_credit_bln"] if c in b.columns]
        if emergency_cols:
            b["total_emergency_bln"] = b[emergency_cols].fillna(0).sum(axis=1)
            b["MAD_score_emergency"]  = mad_normalize(b["total_emergency_bln"], window=MAD_WINDOW)
            # Flag_Emergency: любое появление экстренного кредита > 0 = стресс
            b["Flag_Emergency"] = (b["total_emergency_bln"] > 0).astype(int)

        df["date"] = pd.to_datetime(df["date"])
        df = pd.merge_asof(df.sort_values("date"),
                           b.sort_values("date"),
                           on="date", direction="nearest", tolerance=pd.Timedelta("7d"))
        return df

    # ── fallback: старые данные без спроса ─────────────────────────────────

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
            df["utilization"] = np.nan

        df["MAD_score_rate_spread"] = mad_normalize(df["rate_spread"],  window=MAD_WINDOW)
        df["MAD_score_cover"]       = mad_normalize(df["utilization"],  window=MAD_WINDOW)

        row_num = df["MAD_score_rate_spread"].notna().cumsum()
        df["Flag_Demand"] = (
            (df["MAD_score_rate_spread"].fillna(0) > 2.0) &
            (row_num >= MAD_WINDOW // 2)
        ).astype(int)
        return df
