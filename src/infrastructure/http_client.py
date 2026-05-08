"""
HTTP клиент с поддержкой retries и timeout
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Any, Dict, Optional


class HTTPClient:
    """Обёртка над requests с retry логикой"""

    def __init__(self, timeout: int = 30, max_retries: int = 3):
        self.timeout = timeout
        self.session = self._create_session(max_retries)

    def _create_session(self, max_retries: int) -> requests.Session:
        """Создать сессию с retry стратегией"""
        session = requests.Session()

        retry_strategy = Retry(
            total=max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def get(self, url: str, params: Optional[Dict] = None, **kwargs) -> Dict[str, Any]:
        """GET запрос"""
        try:
            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout,
                **kwargs
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            raise Exception(f"GET request failed: {str(e)}")

    def post(self, url: str, json: Optional[Dict] = None, **kwargs) -> Dict[str, Any]:
        """POST запрос"""
        try:
            response = self.session.post(
                url,
                json=json,
                timeout=self.timeout,
                **kwargs
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            raise Exception(f"POST request failed: {str(e)}")

    def close(self):
        """Закрыть сессию"""
        self.session.close()
