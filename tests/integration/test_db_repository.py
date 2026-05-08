"""
Integration тесты для репозитория БД
"""

from src.infrastructure.storage.repository import Repository


def test_repository_initialization():
    """Тестировать инициализацию репозитория"""
    repo = Repository()

    assert repo is not None
    assert repo.session is not None
