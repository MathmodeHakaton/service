"""
Результат вычисления Liquidity Sentiment Index
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict


@dataclass
class LSIResult:
    """DTO: результат вычисления LSI"""
    value: float  # итоговый LSI 0-1
    status: str  # "normal", "warning", "critical"
    timestamp: datetime

    # Вклады от модулей
    contributions: Dict[str, float] = field(
        default_factory=dict)  # {module_name: contribution}

    # Дополнительные данные
    raw_scores: Dict[str, float] = field(
        default_factory=dict)  # {module_name: raw_score}
