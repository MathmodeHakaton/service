"""
Integration тесты для пайплайна
"""

from src.application.pipeline import Pipeline


def test_pipeline_execute():
    """Тестировать выполнение полного пайплайна"""
    pipeline = Pipeline()
    result = pipeline.execute()

    assert result is not None
    assert 0 <= result.value <= 1
    assert result.status in ["normal", "warning", "critical", "error"]
    assert result.contributions is not None
