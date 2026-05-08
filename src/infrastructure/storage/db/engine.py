"""
Создание и конфигурация SQLAlchemy engine и session factory
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from config.settings import get_settings


def get_engine():
    """Создать SQLAlchemy engine"""
    settings = get_settings()
    engine = create_engine(settings.database_url, echo=False)
    return engine


def get_session_factory() -> sessionmaker:
    """Получить session factory"""
    engine = get_engine()
    return sessionmaker(bind=engine, expire_on_commit=False)


def get_session() -> Session:
    """Получить новую сессию"""
    SessionLocal = get_session_factory()
    return SessionLocal()
