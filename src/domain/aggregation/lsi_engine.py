"""
LSI Engine - агрегирует сигналы модулей в единый индекс
"""

from typing import Dict, List
from datetime import datetime
from ..models.module_signal import ModuleSignal
from ..models.lsi_result import LSIResult
from config.constants import MODULES_WEIGHTS, LSI_THRESHOLD_CRITICAL, LSI_THRESHOLD_WARNING


class LSIEngine:
    """Двигатель вычисления Liquidity Sentiment Index"""

    def __init__(self, weights: Dict[str, float] = None):
        self.weights = weights or MODULES_WEIGHTS

    def compute(self, signals: List[ModuleSignal]) -> LSIResult:
        """
        Вычислить LSI на основе сигналов модулей

        Args:
            signals: список сигналов от модулей

        Returns:
            LSIResult: результат вычисления
        """
        if not signals:
            return LSIResult(
                value=0.5,
                status="error",
                timestamp=datetime.now(),
                contributions={},
                raw_scores={},
            )

        # Вычислить взвешенное среднее
        total_weight = 0
        weighted_sum = 0
        contributions = {}
        raw_scores = {}

        for signal in signals:
            weight = self.weights.get(signal.module_name, 0.2)
            weighted_sum += signal.value * weight
            total_weight += weight
            contributions[signal.module_name] = signal.value * weight
            raw_scores[signal.module_name] = signal.value

        # Нормализовать
        lsi_value = weighted_sum / total_weight if total_weight > 0 else 0.5

        # Определить статус
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
