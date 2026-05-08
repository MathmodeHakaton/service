"""
Unit тесты для модуля M2 (репо)
"""

from src.domain.modules.m2_repo import M2Repo


def test_m2_repo_compute(sample_data):
    """Тестировать вычисление сигнала M2"""
    module = M2Repo()
    signal = module.compute(sample_data)

    assert signal.module_name == "M2_REPO"
    assert 0 <= signal.value <= 1
