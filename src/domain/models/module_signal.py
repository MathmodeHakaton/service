"""
Сигнал модуля анализа
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class ModuleSignal:
    """DTO: сигнал от модуля анализа"""
    module_name: str  # M1, M2, M3, M4, M5
    value: float  # нормализованное значение 0-1
    mad_scores: List[float]  # истории MAD оценок
    flags: List[str]  # аномалии: "normal", "warning", "critical"
    latest_flag: str  # последний флаг
    contribution: float  # вклад в LSI (0-1)
