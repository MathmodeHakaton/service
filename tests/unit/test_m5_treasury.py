"""
Unit тесты для модуля M5 (казначейство)
"""

from src.domain.modules.m5_treasury import M5Treasury


def test_m5_treasury_compute(sample_data):
    """Тестировать вычисление сигнала M5"""
    module = M5Treasury()
    signal = module.compute(sample_data)

    assert signal.module_name == "M5_TREASURY"
    assert 0 <= signal.value <= 1
