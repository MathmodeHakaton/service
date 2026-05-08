"""
Базовый fetcher для получения данных из внешних источников
"""

from abc import ABC, abstractmethod
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class FetcherResult:
    """Результат получения данных"""
    data: Any
    last_updated: datetime
    status: str  # "success", "partial", "error"
    error_message: Optional[str] = None


class BaseFetcher(ABC):
    """Абстрактный базовый класс для всех fetcher-ов"""

    def __init__(self, timeout: int = 30, retries: int = 3):
        self.timeout = timeout
        self.retries = retries

    @abstractmethod
    def fetch(self) -> FetcherResult:
        """Получить данные из внешнего источника"""
        pass

    def _validate_data(self, data: Any) -> bool:
        """Валидация полученных данных"""
        return data is not None and len(data) > 0
