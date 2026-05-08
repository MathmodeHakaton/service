"""
Fetcher для данных Росказны (ЕКС)
"""

from datetime import datetime
from .base import BaseFetcher, FetcherResult


class RoskaznаFetcher(BaseFetcher):
    """Получение данных из API Росказны"""

    def __init__(self, base_url: str = "https://www.roskazna.gov.ru/api/"):
        super().__init__()
        self.base_url = base_url

    def fetch(self) -> FetcherResult:
        """Получить данные ЕКС"""
        try:
            # TODO: реализовать получение данных
            data = {
                "eks": [],
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
