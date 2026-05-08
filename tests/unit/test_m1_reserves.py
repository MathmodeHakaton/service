"""
Unit тесты для модуля M1 (резервы)
"""

from src.domain.modules.m1_reserves import M1Reserves


def test_m1_reserves_compute(sample_data):
    """Тестировать вычисление сигнала M1"""
    module = M1Reserves()
    signal = module.compute(sample_data)

    assert signal.module_name == "M1_RESERVES"
    assert 0 <= signal.value <= 1
    assert signal.latest_flag in ["normal", "warning", "critical", "error"]
    assert signal.contribution >= 0


def test_m1_reserves_empty_data():
    """Тестировать обработку пустых данных"""
    module = M1Reserves()
    signal = module.compute({})

    assert signal.value == 0.5
    assert signal.latest_flag == "error"
    assert signal.contribution == 0.0
