"""
Данные результата получения данных от fetcher
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass
class FetcherResult:
    """DTO: результат работы fetcher"""
    data: Any
    last_updated: datetime
    status: str  # "success", "partial", "error"
    error_message: Optional[str] = None
    source_url: Optional[str] = None
