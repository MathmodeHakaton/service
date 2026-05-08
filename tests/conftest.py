"""
Pytest конфиг и фикстуры
"""

import pytest
from src.infrastructure.fetchers.fake_fetcher import FakeFetcher
from src.infrastructure.storage.repository import Repository


@pytest.fixture
def fake_fetcher():
    """Фикстура для fake fetcher"""
    return FakeFetcher()


@pytest.fixture
def sample_data():
    """Фикстура с тестовыми данными"""
    return {
        "reserves": [100, 101, 102, 103],
        "repo": [50, 51, 52, 53],
        "ruonia": [0.05, 0.051, 0.052, 0.053],
    }
