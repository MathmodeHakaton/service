"""
Fetcher для данных Росказны (ЕКС — Единый казначейский счёт).

Сайт Росказны недоступен через стандартный SSL (использует российский CA).
Используется баланс ликвидности ЦБ РФ (bliquidity) как ground truth —
это данные из CBRFetcher.fetch_bliquidity(), содержащие структурный баланс
банковского сектора (включает операции казначейства).

Данный класс оставлен как адаптер: перенаправляет на CBRFetcher.
"""

import logging
from datetime import datetime

from .base import BaseFetcher, FetcherResult
from .cbr import CBRFetcher

logger = logging.getLogger(__name__)


class RoskaznaFetcher(BaseFetcher):
    """
    Fetcher данных казначейства.
    Росказна SSL недоступна — используем баланс ликвидности ЦБ.
    """

    def __init__(self, cache_dir: str = "./cache/cbr", timeout: int = 60, retries: int = 3):
        super().__init__(timeout=timeout, retries=retries)
        self._cbr = CBRFetcher(cache_dir=cache_dir, timeout=timeout, retries=retries)

    def fetch(self) -> FetcherResult:
        """
        Получить данные о движении средств казначейства.
        Источник: ЦБ РФ bliquidity (структурный баланс ликвидности).
        """
        try:
            df = self._cbr.fetch_bliquidity()
            logger.info("Roskazna (via CBR bliquidity): %d строк", len(df))
            return FetcherResult(
                data={"bliquidity": df, "eks_deposits": None},
                last_updated=datetime.now(),
                status="partial",
                error_message="Росказна SSL недоступна; используются данные ЦБ bliquidity",
            )
        except Exception as e:
            logger.warning("Roskazna/CBR fetch failed: %s", e)
            return FetcherResult(
                data={"bliquidity": None, "eks_deposits": None},
                last_updated=datetime.now(),
                status="error",
                error_message=str(e),
            )
