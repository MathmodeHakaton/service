"""
Fetcher для данных Минфина (ОФЗ)
"""

from datetime import datetime
from .base import BaseFetcher, FetcherResult


class MinfinFetcher(BaseFetcher):
    """Получение данных из API Минфина"""

    def __init__(self, base_url: str = "https://minfin.gov.ru/api/"):
        super().__init__()
        self.base_url = base_url

    def fetch(self) -> FetcherResult:
        """Получить данные ОФЗ"""
        try:
            # TODO: реализовать получение данных
            data = {
                "ofz": [],
            }

            return FetcherResult(
                data=data,
                last_updated=datetime.now(),
                status="success",
            )
        except Exception as e:
            return FetcherResult(
                data=None,
                last_updated=datetime.now(),
                status="error",
                error_message=str(e),
            )
