"""
Unit тесты для модуля M3 (ОФЗ)
"""

from src.domain.modules.m3_ofz import M3OFZ


def test_m3_ofz_compute(sample_data):
    """Тестировать вычисление сигнала M3"""
    module = M3OFZ()
    signal = module.compute(sample_data)

    assert signal.module_name == "M3_OFZ"
    assert 0 <= signal.value <= 1
