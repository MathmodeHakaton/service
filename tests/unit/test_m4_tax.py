"""
Unit тесты для модуля M4 (налоги)
"""

from src.domain.modules.m4_tax import M4Tax


def test_m4_tax_compute(sample_data):
    """Тестировать вычисление сигнала M4"""
    module = M4Tax()
    signal = module.compute(sample_data)

    assert signal.module_name == "M4_TAX"
    assert 0 <= signal.value <= 1
