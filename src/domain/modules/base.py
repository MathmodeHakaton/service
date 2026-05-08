"""
Базовый класс для всех модулей анализа
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
from ..models.module_signal import ModuleSignal


class BaseModule(ABC):
    """Абстрактный базовый класс модуля анализа"""

    def __init__(self, name: str, weight: float = 0.2):
        self.name = name
        self.weight = weight

    @abstractmethod
    def compute(self, data: Dict[str, Any]) -> ModuleSignal:
        """
        Вычислить сигнал на основе данных

        Args:
            data: словарь с данными от fetchers

        Returns:
            ModuleSignal: сигнал модуля
        """
        pass

    def _validate_data(self, data: Dict[str, Any]) -> bool:
        """Валидация входных данных"""
        return data is not None and len(data) > 0
