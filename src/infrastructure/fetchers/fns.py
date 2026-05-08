"""
Fetcher для данных ФНС (налоговый календарь)
"""

from datetime import datetime
from .base import BaseFetcher, FetcherResult


class FNSFetcher(BaseFetcher):
    """Получение данных из API ФНС"""

    def __init__(self, base_url: str = "https://service.nalog.ru/api/"):
        super().__init__()
        self.base_url = base_url

    def fetch(self) -> FetcherResult:
        """Получить данные налогового календаря"""
        try:
            # TODO: реализовать получение данных
            data = {
                "tax_calendar": [],
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
