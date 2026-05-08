"""
Модуль М5: ЕКС (единая казначейская система)
"""

from typing import Dict, Any
from .base import BaseModule
from ..models.module_signal import ModuleSignal


class M5Treasury(BaseModule):
    """Модуль анализа казначейской системы"""

    def __init__(self):
        super().__init__(name="M5_TREASURY", weight=0.15)

    def compute(self, data: Dict[str, Any]) -> ModuleSignal:
        """Анализ ЕКС"""
        if not self._validate_data(data):
            return ModuleSignal(
                module_name=self.name,
                value=0.5,
                mad_scores=[],
                flags=[],
                latest_flag="error",
                contribution=0.0,
            )

        # TODO: реализовать логику анализа казначейства
        return ModuleSignal(
            module_name=self.name,
            value=0.5,
            mad_scores=[0.5],
            flags=["normal"],
            latest_flag="normal",
            contribution=self.weight,
        )
