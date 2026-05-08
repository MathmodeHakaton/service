"""
Константы приложения
"""

# MAD (Mean Absolute Deviation) параметры
MAD_WINDOW_YEARS = 3
MAD_ANOMALY_THRESHOLD = 3  # кол-во стандартных отклонений

# LSI пороги
LSI_THRESHOLD_CRITICAL = 0.8
LSI_THRESHOLD_WARNING = 0.6
LSI_THRESHOLD_NORMAL = 0.4

# Модули (веса)
MODULES_WEIGHTS = {
    "M1_RESERVES": 0.25,
    "M2_REPO": 0.25,
    "M3_OFZ": 0.20,
    "M4_TAX": 0.15,
    "M5_TREASURY": 0.15,
}

# API timeouts
REQUEST_TIMEOUT_SECONDS = 30
RETRY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 5

# Кэш
CACHE_TTL_HOURS = 24
SAMPLE_SIZE_FOR_TESTS = 100
