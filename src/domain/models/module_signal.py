"""
Сигнал модуля анализа
"""

from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class ModuleSignal:
    """DTO: сигнал от модуля анализа"""
    module_name: str       # M1_RESERVES, M2_REPO, M3_OFZ, M4_TAX, M5_TREASURY
    signals: Dict[str, float] = field(default_factory=dict)  # именованные сигналы по ТЗ
    latest_flag: str = "normal"   # "normal" / "warning" / "critical" / "error"
    value: float = 0.0     # вычисляется LSIEngine из signals (не модулем)
    contribution: float = 0.0  # вычисляется LSIEngine
