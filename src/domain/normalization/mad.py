"""
MAD-нормализация (Median Absolute Deviation) по скользящему окну.

Исправлено относительно оригинальной заглушки:
  - использует median(), а не mean() — устойчиво к выбросам
  - формула: (x - median) / (1.4826 * MAD) — стандартная sigma-оценка
  - результат clip(±10) — защита от артефактов при малой дисперсии окна
"""

import numpy as np
import pandas as pd
from typing import List, Optional

MAD_CLIP = 10.0


def mad_normalize(series: pd.Series, window: int = 36) -> pd.Series:
    """
    MAD-нормализация по скользящему окну (pandas Series).

    Args:
        series: временной ряд значений
        window: размер окна (мес. или дней в зависимости от данных)

    Returns:
        Series с нормализованными значениями в диапазоне [-10, 10].
        0 = историческая норма, +3 = 3σ выше нормы (стресс), -3 = ниже нормы.
    """
    def _mad_score(arr: np.ndarray) -> float:
        clean = arr[~np.isnan(arr)]
        if len(clean) < 5:
            return np.nan
        med = np.median(clean)
        mad = np.median(np.abs(clean - med))
        if mad == 0:
            return 0.0
        score = (clean[-1] - med) / (1.4826 * mad)
        return float(np.clip(score, -MAD_CLIP, MAD_CLIP))

    return series.rolling(window=window, min_periods=max(10, window // 4)).apply(
        _mad_score, raw=True
    )


def mad_to_score(mad_value: float, invert: bool = False) -> float:
    """
    Перевод MAD z-score в оценку [0, 1] через сигмоиду.

    Args:
        mad_value: значение MAD z-score
        invert:    если True, инвертирует сигнал (дефицит = стресс)

    Returns:
        float в [0, 1]: 0.5 = норма, >0.7 = стресс, <0.3 = профицит
    """
    if np.isnan(mad_value):
        return 0.5
    v = -mad_value if invert else mad_value
    return float(1.0 / (1.0 + np.exp(-v)))


class MADNormalizer:
    """Объектный интерфейс для совместимости с архитектурой service/."""

    def __init__(self, window_years: int = 3):
        self.window_years = window_years
        self.window_size = window_years * 252

    def compute_series(self, series: pd.Series, window: Optional[int] = None) -> pd.Series:
        """MAD-нормализация pandas Series."""
        return mad_normalize(series, window=window or self.window_size)

    def compute(self, values: List[float], window: Optional[int] = None) -> List[float]:
        """MAD-нормализация списка; возвращает список z-score."""
        s = pd.Series(values, dtype=float)
        result = mad_normalize(s, window=window or self.window_size)
        return result.tolist()

    def get_anomaly_flag(self, z_score: float, threshold: float = 3.0) -> str:
        """Определить текстовый флаг аномалии по z-score."""
        if np.isnan(z_score):
            return "error"
        if abs(z_score) > threshold:
            return "critical"
        elif abs(z_score) > threshold * 0.67:
            return "warning"
        return "normal"
