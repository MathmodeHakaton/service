"""
LSI Engine — агрегирует сырые MAD-признаки модулей в единый индекс.

Входные данные: Dict[str, pd.DataFrame] — признаки каждого модуля по ТЗ.
Метод агрегации: взвешенная сумма с sigmoid (явная формула, интерпретируема).
M4_TAX — мультипликатор Seasonal_Factor, не аддитивен.

Вклад каждого модуля считается из сырых MAD-сигналов (не из sub-score),
что соответствует требованию ТЗ о ML-агрегации с интерпретируемостью.
"""

import numpy as np
from datetime import datetime
from typing import Dict

import pandas as pd

from ..models.lsi_result import LSIResult
from config.constants import LSI_THRESHOLD_CRITICAL, LSI_THRESHOLD_WARNING

# Веса пропорциональны SNR на стресс-эпизодах (Dec 2014, Feb 2022, Aug 2023)
# SNR: M1=3.62, M2=3.50, M3=1.42, M5=0.82 → сумма=9.36
WEIGHTS = {
    "M1_RESERVES": 0.387,
    "M2_REPO":     0.374,
    "M3_OFZ":      0.152,
    "M5_TREASURY": 0.088,
}


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + np.exp(-float(x)))


def _latest(df: pd.DataFrame) -> dict:
    """Возвращает последнюю непустую строку как dict."""
    if df is None or df.empty:
        return {}
    return df.dropna(how="all").iloc[-1].to_dict() if not df.dropna(how="all").empty else {}


class LSIEngine:

    def __init__(self, weights: Dict[str, float] = None):
        self.weights = weights or WEIGHTS

    def compute(self, signals: Dict[str, pd.DataFrame]) -> LSIResult:
        """
        signals: {module_name: DataFrame с признаками по ТЗ}

        Возвращает LSIResult с value в [0, 1].
        """
        m1 = _latest(signals.get("M1_RESERVES"))
        m2 = _latest(signals.get("M2_REPO"))
        m3 = _latest(signals.get("M3_OFZ"))
        m4 = _latest(signals.get("M4_TAX"))
        m5 = _latest(signals.get("M5_TREASURY"))

        # ── per-module score из сырых MAD-признаков ──────────────────────────
        score_m1 = self._score_m1(m1)
        score_m2 = self._score_m2(m2)
        score_m3 = self._score_m3(m3)   # None если нет данных
        score_m5 = self._score_m5(m5)

        seasonal_factor = float(m4.get("Seasonal_Factor", 1.0) or 1.0)

        # ── взвешенная сумма с ренормализацией при отсутствии модуля ─────────
        available = {
            "M1_RESERVES": score_m1,
            "M2_REPO":     score_m2,
            "M3_OFZ":      score_m3,
            "M5_TREASURY": score_m5,
        }
        active = {k: v for k, v in available.items() if v is not None}
        if not active:
            active = {"M1_RESERVES": 0.5}

        w_total = sum(self.weights.get(k, 0.25) for k in active)
        base_lsi = sum(self.weights.get(k, 0.25) / w_total * v for k, v in active.items())

        lsi_value = float(np.clip(base_lsi * seasonal_factor, 0.0, 1.0))

        contributions = {
            k: round(self.weights.get(k, 0.25) / w_total * v, 4)
            for k, v in active.items()
        }
        contributions["M4_TAX_multiplier"] = round(seasonal_factor, 2)

        raw_scores = {k: round(v, 4) for k, v in active.items()}
        raw_scores["M4_TAX"] = round(seasonal_factor, 2)

        if lsi_value >= LSI_THRESHOLD_CRITICAL:
            status = "critical"
        elif lsi_value >= LSI_THRESHOLD_WARNING:
            status = "warning"
        else:
            status = "normal"

        return LSIResult(
            value=lsi_value,
            status=status,
            timestamp=datetime.now(),
            contributions=contributions,
            raw_scores=raw_scores,
        )

    # ── формулы агрегации по модулю ──────────────────────────────────────────

    def _score_m1(self, r: dict) -> float:
        """MAD_score_RUONIA × 0.60 + MAD_score_спред × 0.40 + бонус флагов."""
        mad_ru  = float(r.get("MAD_score_RUONIA", 0) or 0)
        mad_sp  = float(r.get("MAD_score_спред",  0) or 0)
        flag_ak = int(r.get("Flag_AboveKey",    0) or 0)
        flag_ep = int(r.get("Flag_EndOfPeriod", 0) or 0)
        s = _sigmoid(0.60 * mad_ru + 0.40 * mad_sp)
        if flag_ak: s = min(s + 0.08, 1.0)
        if flag_ep: s = min(s + 0.05, 1.0)
        return float(np.clip(s, 0.0, 1.0))

    def _score_m2(self, r: dict) -> float:
        """MAD_score_rate_spread + бонус Flag_Demand."""
        mad_r  = float(r.get("MAD_score_rate_spread", 0) or 0)
        flag_d = int(r.get("Flag_Demand", 0) or 0)
        s = _sigmoid(mad_r)
        if flag_d: s = min(s + 0.10, 1.0)
        return float(np.clip(s, 0.0, 1.0))

    def _score_m3(self, r: dict):
        """−MAD_score_cover × 0.65 + MAD_score_yield_spread × 0.35.
        Флаги Nedospros/Perespros выводятся как признаки, но не влияют на score —
        MAD уже захватывает низкий bid_cover через инверсию знака.
        """
        mad_bc = r.get("MAD_score_cover")
        mad_yl = r.get("MAD_score_yield_spread")
        if mad_bc is None or (isinstance(mad_bc, float) and np.isnan(mad_bc)):
            return None
        mad_bc = float(mad_bc or 0)
        mad_yl = float(mad_yl or 0)
        return float(np.clip(_sigmoid(0.65 * (-mad_bc) + 0.35 * mad_yl), 0.0, 1.0))

    def _score_m5(self, r: dict) -> float:
        """MAD_score_ЦБ × 0.88 + MAD_score_Росказна × 0.12 + Flag_Budget_Drain."""
        mad_b  = float(r.get("MAD_score_ЦБ",       0) or 0)
        mad_rk = float(r.get("MAD_score_Росказна",  0) or 0)
        flag   = int(r.get("Flag_Budget_Drain",     0) or 0)
        s = _sigmoid(0.88 * mad_b + 0.12 * mad_rk)
        if flag: s = min(s + 0.15, 1.0)
        return float(np.clip(s, 0.0, 1.0))
