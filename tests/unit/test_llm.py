"""
Unit тесты для LLM
"""

from src.domain.llm.local_model import LocalLLM
from src.domain.llm.prompt_builder import PromptBuilder
from src.domain.models.lsi_result import LSIResult
from datetime import datetime


def test_prompt_builder_lsi_analysis():
    """Тестировать построение промпта анализа LSI"""

    lsi_result = LSIResult(
        value=0.6,
        status="warning",
        timestamp=datetime.now(),
        contributions={"M1": 0.15, "M2": 0.15,
                       "M3": 0.15, "M4": 0.10, "M5": 0.05},
    )

    prompt = PromptBuilder.build_lsi_analysis_prompt(lsi_result)

    assert "0.60%" in prompt or "60%" in prompt
    assert "warning" in prompt.lower()
    assert "M1" in prompt


def test_local_llm_availability():
    """Тестировать проверку доступности LLM"""
    llm = LocalLLM()
    # Просто проверим, что метод не вызывает ошибок
    availability = llm.is_available()
    assert isinstance(availability, bool)
