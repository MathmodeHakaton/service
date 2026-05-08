"""
Модуль М2: Репо операции
"""

from typing import Dict, Any
from .base import BaseModule
from ..models.module_signal import ModuleSignal


class M2Repo(BaseModule):
    """Модуль анализа репо рынка"""

    def __init__(self):
        super().__init__(name="M2_REPO", weight=0.25)

    def compute(self, data: Dict[str, Any]) -> ModuleSignal:
        """Анализ репо"""
        if not self._validate_data(data):
            return ModuleSignal(
                module_name=self.name,
                value=0.5,
                mad_scores=[],
                flags=[],
                latest_flag="error",
                contribution=0.0,
            )

        # TODO: реализовать логику анализа репо
        return ModuleSignal(
            module_name=self.name,
            value=0.5,
            mad_scores=[0.5],
            flags=["normal"],
            latest_flag="normal",
            contribution=self.weight,
        )
