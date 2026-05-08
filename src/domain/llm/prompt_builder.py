"""
Построитель структурированных промптов для LLM
"""

from datetime import datetime
from ..models.lsi_result import LSIResult


class PromptBuilder:
    """Строитель промптов для аналитика"""

    @staticmethod
    def build_lsi_analysis_prompt(lsi_result: LSIResult) -> str:
        """Построить промпт для анализа LSI"""

        contributions_str = "\n".join(
            f"- {module}: {contrib:.2%}"
            for module, contrib in lsi_result.contributions.items()
        )

        prompt = f"""
Ты финансовый аналитик по ликвидности российского рубля.

Проанализируй следующие данные Liquidity Sentiment Index (LSI):

**Основные метрики:**
- LSI Value: {lsi_result.value:.2%}
- Status: {lsi_result.status}
- Timestamp: {lsi_result.timestamp}

**Вклады модулей:**
{contributions_str}

На основе этих данных:
1. Дай краткий анализ состояния ликвидности
2. Выдели ключевые риски
3. Предложи рекомендации
"""
        return prompt.strip()

    @staticmethod
    def build_chat_context_prompt(lsi_result: LSIResult, chat_history: list) -> str:
        """Построить контекст для чата"""

        history_str = "\n".join(
            f"{msg['role'].upper()}: {msg['content']}"
            for msg in chat_history[-5:]  # последние 5 сообщений
        )

        prompt = f"""
Контекст анализа ликвидности:
- Текущий LSI: {lsi_result.value:.2%}
- Статус: {lsi_result.status}

История обсуждения:
{history_str}

Продолжи обсуждение, отвечая на вопрос пользователя.
"""
        return prompt.strip()
