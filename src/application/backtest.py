"""
BacktestRunner: запуск исторического backtesting
"""

from typing import List, Dict
from datetime import datetime
from src.domain.models.lsi_result import LSIResult


class BacktestRunner:
    """Запуск backtesting на исторических данных"""

    def __init__(self, start_year: int = 2014, end_year: int = 2023):
        self.start_year = start_year
        self.end_year = end_year

    def run(self) -> List[LSIResult]:
        """Запустить backtesting"""
        results = []

        for year in range(self.start_year, self.end_year + 1):
            # TODO: реализовать загрузку исторических данных
            # и вычисление LSI для каждого года

            result = LSIResult(
                value=0.5,
                status="normal",
                timestamp=datetime(year, 1, 1),
                contributions={},
                raw_scores={},
            )
            results.append(result)

        return results

    def get_metrics(self, results: List[LSIResult]) -> Dict[str, float]:
        """Вычислить метрики backtesting"""

        if not results:
            return {}

        values = [r.value for r in results]

        # TODO: реализовать вычисление Sharpe ratio, max drawdown, etc.

        metrics = {
            "sharpe_ratio": 1.5,
            "max_drawdown": 0.15,
            "total_return": 0.25,
            "win_rate": 0.65,
        }

        return metrics
