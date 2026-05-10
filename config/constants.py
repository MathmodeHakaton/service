"""
Константы приложения
"""

from datetime import timedelta


# MAD (Mean Absolute Deviation) параметры
MAD_WINDOW_YEARS = 3
MAD_ANOMALY_THRESHOLD = 3  # кол-во стандартных отклонений

# LSI пороги (0-1 scale): GREEN < 0.40, YELLOW 0.40-0.70, RED > 0.70
LSI_THRESHOLD_CRITICAL = 0.70
LSI_THRESHOLD_WARNING = 0.40
LSI_THRESHOLD_NORMAL = 0.40

# Модули (веса) — M4_TAX не входит: используется как мультипликатор
# Веса пропорциональны SNR каждого модуля на стресс-эпизодах (Dec 2014, Feb 2022, Aug 2023)
# SNR: M1=3.62, M2=3.50, M3=1.42, M5=0.82 → сумма=9.36
MODULES_WEIGHTS = {
    "M1_RESERVES": 0.387,
    "M2_REPO":     0.374,
    "M3_OFZ":      0.152,
    "M5_TREASURY": 0.088,
}

# API timeouts
REQUEST_TIMEOUT_SECONDS = 30
RETRY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 5

# Кэш
CACHE_TTL_HOURS = 24
SAMPLE_SIZE_FOR_TESTS = 100


CACHE_TTL = {
    "minfin_ofz":        timedelta(hours=12),
    "cbr_repo":          timedelta(hours=6),
    "cbr_reserves":      timedelta(hours=24),
    "cbr_ruonia":        timedelta(hours=6),
    "roskazna_eks":      timedelta(hours=24),
    "fns_tax_calendar":  timedelta(days=7),
}
