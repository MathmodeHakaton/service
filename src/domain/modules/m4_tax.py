"""
Модуль М4: Налоговый календарь ФНС
"""

from typing import Dict, Any
from .base import BaseModule
from ..models.module_signal import ModuleSignal


class M4Tax(BaseModule):
    """Модуль анализа налогового календаря"""

    def __init__(self):
        super().__init__(name="M4_TAX", weight=0.15)

    def compute(self, data: Dict[str, Any]) -> ModuleSignal:
        """Анализ налогового календаря"""
        if not self._validate_data(data):
            return ModuleSignal(
                module_name=self.name,
                value=0.5,
                mad_scores=[],
                flags=[],
                latest_flag="error",
                contribution=0.0,
            )

        # TODO: реализовать логику анализа налогов
        return ModuleSignal(
            module_name=self.name,
            value=0.5,
            mad_scores=[0.5],
            flags=["normal"],
            latest_flag="normal",
            contribution=self.weight,
        )
