"""
Модуль М3: ОФЗ (облигации федерального займа)
"""

from typing import Dict, Any
from .base import BaseModule
from ..models.module_signal import ModuleSignal


class M3OFZ(BaseModule):
    """Модуль анализа рынка ОФЗ"""

    def __init__(self):
        super().__init__(name="M3_OFZ", weight=0.20)

    def compute(self, data: Dict[str, Any]) -> ModuleSignal:
        """Анализ ОФЗ"""
        if not self._validate_data(data):
            return ModuleSignal(
                module_name=self.name,
                value=0.5,
                mad_scores=[],
                flags=[],
                latest_flag="error",
                contribution=0.0,
            )

        # TODO: реализовать логику анализа ОФЗ
        return ModuleSignal(
            module_name=self.name,
            value=0.5,
            mad_scores=[0.5],
            flags=["normal"],
            latest_flag="normal",
            contribution=self.weight,
        )
