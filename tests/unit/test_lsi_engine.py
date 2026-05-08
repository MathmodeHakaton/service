"""
Unit тесты для LSI Engine
"""

from src.domain.aggregation.lsi_engine import LSIEngine
from src.domain.models.module_signal import ModuleSignal


def test_lsi_engine_compute():
    """Тестировать вычисление LSI"""
    engine = LSIEngine()

    signals = [
        ModuleSignal("M1_RESERVES", 0.5, [], [], "normal", 0.25),
        ModuleSignal("M2_REPO", 0.6, [], [], "normal", 0.25),
        ModuleSignal("M3_OFZ", 0.7, [], [], "warning", 0.20),
        ModuleSignal("M4_TAX", 0.4, [], [], "normal", 0.15),
        ModuleSignal("M5_TREASURY", 0.5, [], [], "normal", 0.15),
    ]

    result = engine.compute(signals)

    assert 0 <= result.value <= 1
    assert result.status in ["normal", "warning", "critical"]
    assert len(result.contributions) == 5


def test_lsi_engine_empty_signals():
    """Тестировать LSI с пустыми сигналами"""
    engine = LSIEngine()
    result = engine.compute([])

    assert result.value == 0.5
    assert result.status == "error"
