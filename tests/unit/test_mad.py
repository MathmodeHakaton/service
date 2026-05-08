"""
Unit тесты для MAD нормализатора
"""

from src.domain.normalization.mad import MADNormalizer


def test_mad_normalizer_compute():
    """Тестировать вычисление MAD"""
    normalizer = MADNormalizer(window_years=1)

    values = [100, 101, 102, 100, 99, 98, 100, 101]
    result = normalizer.compute(values)

    assert len(result) == len(values)
    assert all(0 <= v <= 1 for v in result)


def test_mad_normalizer_anomaly_flag():
    """Тестировать определение флага аномалии"""
    normalizer = MADNormalizer()

    # Нормальное
    assert normalizer.get_anomaly_flag(1.5) == "normal"

    # Предупреждение
    assert normalizer.get_anomaly_flag(2.0) == "warning"

    # Критическое
    assert normalizer.get_anomaly_flag(3.5) == "critical"
