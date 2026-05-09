"""
Базовый класс для всех модулей анализа
"""

from abc import ABC, abstractmethod
from typing import Dict, Any

import pandas as pd


class BaseModule(ABC):

    def __init__(self, name: str, weight: float = 0.2):
        self.name   = name
        self.weight = weight

    @abstractmethod
    def compute(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Вычислить признаки модуля из сырых данных.

        Returns:
            pd.DataFrame с колонкой 'date' и признаками по ТЗ.
            Пустой DataFrame если данных нет.
        """
        pass
