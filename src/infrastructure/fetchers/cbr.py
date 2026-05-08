"""
Fetcher для данных ЦБ РФ (резервы, репо, RUONIA)
"""

from datetime import datetime
from .base import BaseFetcher, FetcherResult


class CBRFetcher(BaseFetcher):
    """Получение данных из API ЦБ РФ"""

    def __init__(self, base_url: str = "https://www.cbr.ru/dev/api/"):
        super().__init__()
        self.base_url = base_url

    def fetch(self) -> FetcherResult:
        """Получить данные резервов, репо, RUONIA"""
        try:
            # TODO: реализовать получение данных
            data = {
                "reserves": [],
                "repo": [],
                "ruonia": [],
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
