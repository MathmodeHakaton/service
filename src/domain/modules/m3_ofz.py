"""
М3 — Размещение ОФЗ Минфин

Выход по ТЗ (pd.DataFrame):
  date                  — дата аукциона
  MAD_score_cover       — MAD(bid_cover = спрос/предложение, окно 36), SNR=1.63
  MAD_score_yield_spread— MAD(отклонение доходности от кривой, окно 36), SNR=1.02
  Flag_Nedospros        — bid_cover < 1.2  (по ТЗ: cover < 1.2)
  Flag_Perespros        — bid_cover > 2.0  (по ТЗ: cover > 2.0)
"""

from typing import Dict, Any

import numpy as np
import pandas as pd

from .base import BaseModule
from ..normalization.mad import mad_normalize

MAD_WINDOW           = 36
BID_COVER_STRESS     = 1.2
COVER_HIGH_THRESHOLD = 2.0
YIELD_SPREAD_WINDOW  = 52

TZ_COLUMNS = ["date", "MAD_score_cover", "MAD_score_yield_spread",
               "Flag_Nedospros", "Flag_Perespros"]


class M3OFZ(BaseModule):

    def __init__(self):
        super().__init__(name="M3_OFZ", weight=0.25)

    def compute(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        data["ofz"] — DataFrame: date, auction_format, offer_volume,
                      demand_volume, placement_volume, avg_yield

        Returns:
            DataFrame с колонками по ТЗ.
        """
        ofz_df = data.get("ofz", pd.DataFrame())

        if ofz_df is None or (isinstance(ofz_df, pd.DataFrame) and ofz_df.empty):
            return pd.DataFrame(columns=TZ_COLUMNS)

        df = self._calculate(ofz_df)
        if df.empty:
            return pd.DataFrame(columns=TZ_COLUMNS)

        out = [c for c in TZ_COLUMNS if c in df.columns]
        return df[out].copy()

    # ── внутренние вычисления ───────────────────────────────────────────────

    def _calculate(self, ofz_df: pd.DataFrame) -> pd.DataFrame:
        df = ofz_df.copy().sort_values("date").reset_index(drop=True)

        is_auction = df.get(
            "auction_format", pd.Series("", index=df.index)
        ).str.upper().str.contains("АУКЦИОН|AUCTION", na=False)
        df["is_auction"] = is_auction

        # bid_cover = спрос / предложение (по ТЗ: cover ratio = спрос/предложение)
        if "bid_cover" not in df.columns:
            if "demand_volume" in df.columns and "offer_volume" in df.columns:
                df["bid_cover"] = np.where(
                    is_auction & df["offer_volume"].notna() & (df["offer_volume"] > 0),
                    df["demand_volume"] / df["offer_volume"], np.nan
                )

        auctions     = df[is_auction].copy()
        bc_series    = auctions.get("bid_cover",  pd.Series(np.nan, index=auctions.index))
        yl_series    = auctions.get("avg_yield",  pd.Series(np.nan, index=auctions.index))

        # yield_spread = отклонение от скользящей средней ≈ спред к кривой ОФЗ
        yield_baseline = yl_series.rolling(window=YIELD_SPREAD_WINDOW, min_periods=4).mean()
        yield_spread   = yl_series - yield_baseline

        # MAD_score_cover: по ТЗ — cover ratio (bid_cover для РФ)
        auctions["MAD_score_cover"]        = mad_normalize(bc_series,    window=MAD_WINDOW)
        auctions["MAD_score_yield_spread"] = mad_normalize(yield_spread,  window=MAD_WINDOW)

        df = df.merge(
            auctions[["date", "MAD_score_cover", "MAD_score_yield_spread"]],
            on="date", how="left"
        )

        bid_cover_col     = df.get("bid_cover", pd.Series(np.nan, index=df.index))
        df["Flag_Nedospros"] = (bid_cover_col < BID_COVER_STRESS).fillna(False).astype(int)
        df["Flag_Perespros"] = (bid_cover_col > COVER_HIGH_THRESHOLD).fillna(False).astype(int)
        return df
