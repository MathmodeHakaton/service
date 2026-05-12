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
        "DATABASE_URL", "postgresql://localhost:5432/postgres"
    )

    # APIs
    cbr_api_base_url: str = "https://www.cbr.ru/dev/api/"
    minfin_api_base_url: str = "https://minfin.gov.ru/api/"
    roskazna_api_base_url: str = "https://www.roskazna.gov.ru/api/"
    fns_api_base_url: str = "https://service.nalog.ru/api/"

    # Cache
    cache_dir: str = "./cache"
    cache_ttl_hours: int = 24

    # LLM — Yandex AI Studio
    # Получить ключи: https://yandex.cloud/ai-studio
    yandex_api_key: str = os.getenv("YANDEX_API_KEY", "")
    yandex_folder_id: str = os.getenv("YANDEX_FOLDER_ID", "")
    # Имя модели задаётся в формате "<name>/<version>", URL строится как
    # gpt://{folder_id}/<name>/<version>
    yandex_model_commentary: str = "yandexgpt-5-lite/latest"  # автокомментарий
    yandex_model_chat: str = "yandexgpt-5-pro/latest"  # RAG-чат
    yandex_base_url: str = "https://ai.api.cloud.yandex.net/v1"

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
