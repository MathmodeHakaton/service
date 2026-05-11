"""
Fake fetcher для первого дня разработки (тестирование)
"""

from datetime import datetime
from .base import BaseFetcher, FetcherResult


class FakeFetcher(BaseFetcher):
    """Fetcher с тестовыми данными"""

    def fetch(self) -> FetcherResult:
        """Вернуть тестовые данные"""
        data = {
            "reserves": [100, 101, 102, 103],
            "repo": [50, 51, 52, 53],
            "ruonia": [0.05, 0.051, 0.052, 0.053],
            "ofz": [80, 81, 82, 83],
            "eks": [200, 201, 202, 203],
            "tax_calendar": [1000, 1100, 1050, 900],
        }

        return FetcherResult(
            data=data,
            last_updated=datetime.now(),
            status="success",
            source_url=None,
        )
