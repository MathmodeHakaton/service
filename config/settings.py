"""
Настройки приложения из переменных окружения
"""

import os
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Основные настройки приложения"""

    # Database
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://localhost:5432/postgres")

    # APIs
    cbr_api_base_url: str = "https://www.cbr.ru/dev/api/"
    minfin_api_base_url: str = "https://minfin.gov.ru/api/"
    roskazna_api_base_url: str = "https://www.roskazna.gov.ru/api/"
    fns_api_base_url: str = "https://service.nalog.ru/api/"

    # Cache
    cache_dir: str = "./cache"
    cache_ttl_hours: int = 24

    # LLM
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "llama2"

    # Logging
    log_level: str = "INFO"

    # Application
    mad_window_years: int = 3
    lsi_threshold: float = 0.7

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Получить глобальный объект настроек"""
    return Settings()
