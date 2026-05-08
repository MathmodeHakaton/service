"""
Нормализация данных методом MAD (Mean Absolute Deviation)
"""

from typing import List
import statistics


class MADNormalizer:
    """Нормализатор на основе MAD"""

    def __init__(self, window_years: int = 3):
        self.window_years = window_years
        self.window_size = window_years * 252  # торговые дни

    def compute(self, values: List[float]) -> List[float]:
        """
        Вычислить MAD-нормализованные значения

        Args:
            values: список исторических значений

        Returns:
            список нормализованных значений (0-1)
        """
        if not values or len(values) < self.window_size:
            return [0.5] * len(values)

        normalized = []
        for i in range(len(values)):
            # Получить окно данных
            start = max(0, i - self.window_size)
            window = values[start:i+1]

            if len(window) < 2:
                normalized.append(0.5)
                continue

            # Вычислить MAD
            mean = statistics.mean(window)
            mad = statistics.mean(abs(x - mean) for x in window)

            if mad == 0:
                normalized.append(0.5)
            else:
                # Нормализовать к [0, 1]
                z_score = (values[i] - mean) / mad if mad > 0 else 0
                normalized_value = 1 / (1 + abs(z_score))  # sigmoid-like
                normalized.append(normalized_value)

        return normalized

    def get_anomaly_flag(self, z_score: float, threshold: int = 3) -> str:
        """Определить флаг аномалии"""
        if abs(z_score) > threshold:
            return "critical"
        elif abs(z_score) > threshold * 0.66:
            return "warning"
        else:
            return "normal"
