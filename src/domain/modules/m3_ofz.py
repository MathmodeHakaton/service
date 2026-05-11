"""
М3 — Размещение ОФЗ Минфин

Выход по ТЗ (pd.DataFrame):
  date                   — дата аукциона
  MAD_score_cover        — MAD(cover_ratio = спрос/размещение, окно 36), SNR=1.63
  MAD_score_yield_spread — MAD(отклонение доходности от скользящей средней, окно 36)
  Flag_Nedospros         — cover_ratio < 1.2  (мало спроса относительно размещённого)
  Flag_Perespros         — cover_ratio > 2.0  (переспрос, избыток ликвидности)

Два вида покрытия:
  bid_cover   = спрос / предложение  — объёмный, норма для РФ < 1.0 (лимит всегда большой)
  cover_ratio = спрос / размещение   — ценовой, всегда >= 1, используется для MAD и флагов
"""

from typing import Dict, Any

import numpy as np
import pandas as pd

from src.domain.modules.base import BaseModule
from src.domain.normalization.mad import mad_normalize

MAD_WINDOW = 36
COVER_STRESS = 1.2
COVER_HIGH = 2.0
YIELD_SPREAD_WINDOW = 52

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

    def _calculate(self, ofz_df: pd.DataFrame) -> pd.DataFrame:
        df = ofz_df.copy().sort_values("date").reset_index(drop=True)

        is_auction = df.get(
            "auction_format", pd.Series("", index=df.index)
        ).str.upper().str.contains("АУКЦИОН|AUCTION", na=False)
        df["is_auction"] = is_auction

        # cover_ratio = спрос / размещение
        if "cover_ratio" not in df.columns:
            if "demand_volume" in df.columns and "placement_volume" in df.columns:
                df["cover_ratio"] = np.where(
                    is_auction & df["placement_volume"].notna() & (
                        df["placement_volume"] > 0),
                    df["demand_volume"] / df["placement_volume"], np.nan
                )

        # bid_cover = спрос / предложение
        if "bid_cover" not in df.columns:
            if "demand_volume" in df.columns and "offer_volume" in df.columns:
                df["bid_cover"] = np.where(
                    is_auction & df["offer_volume"].notna() & (
                        df["offer_volume"] > 0),
                    df["demand_volume"] / df["offer_volume"], np.nan
                )

        auctions = df[is_auction].copy()

        # MAD_score_cover: по cover_ratio
        cr_series = auctions.get(
            "cover_ratio", pd.Series(np.nan, index=auctions.index))
        yl_series = auctions.get(
            "avg_yield",   pd.Series(np.nan, index=auctions.index))

        yield_baseline = yl_series.rolling(
            window=YIELD_SPREAD_WINDOW, min_periods=4).mean()
        yield_spread = yl_series - yield_baseline

        auctions["MAD_score_cover"] = mad_normalize(
            cr_series,   window=MAD_WINDOW)
        auctions["MAD_score_yield_spread"] = mad_normalize(
            yield_spread, window=MAD_WINDOW)

        df["MAD_score_cover"] = np.nan
        df["MAD_score_yield_spread"] = np.nan
        df.loc[auctions.index, "MAD_score_cover"] = auctions["MAD_score_cover"].values
        df.loc[auctions.index,
               "MAD_score_yield_spread"] = auctions["MAD_score_yield_spread"].values

        cr_col = df.get("cover_ratio", pd.Series(np.nan, index=df.index))
        df["Flag_Nedospros"] = (
            cr_col < COVER_STRESS).fillna(False).astype(int)
        df["Flag_Perespros"] = (cr_col > COVER_HIGH).fillna(False).astype(int)
        return df
