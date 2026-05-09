from abc import ABC, abstractmethod
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Optional
import requests
import pandas as pd
import logging
from io import BytesIO

from config.constants import DEFAULT_FETCH_TIMEOUT, DEFAULT_FETCH_RETRIES

logger = logging.getLogger(__name__)


@dataclass
class FetcherResult:
    """Результат получения данных"""
    data: Any
    last_updated: datetime
    status: str  # "success", "partial", "error"
    error_message: Optional[str] = None


class BaseFetcher(ABC):
    """Абстрактный базовый класс для всех fetcher-ов"""

    def __init__(self, timeout: int = DEFAULT_FETCH_TIMEOUT, retries: int = DEFAULT_FETCH_RETRIES):
        self.timeout = timeout
        self.retries = retries

    @abstractmethod
    def fetch(self) -> FetcherResult:
        """Получить данные из внешнего источника"""
        pass

    def _download_excel(self, url: str) -> Optional[pd.DataFrame]:
        """Загрузить Excel файл с повторными попытками"""
        for attempt in range(self.retries):
            try:
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                df = pd.read_excel(BytesIO(response.content))
                return df
            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"Attempt {attempt + 1}/{self.retries} failed: {e}")
                if attempt == self.retries - 1:
                    logger.error(f"Failed to download from {url}: {e}")
                    return None
        return None

    def _validate_data(self, data: Any) -> tuple[bool, Optional[str]]:
        """Валидация полученных данных"""
        if data is None:
            return False, "Data is None"
        if isinstance(data, list) and len(data) == 0:
            return False, "Data list is empty"
        return True, None
