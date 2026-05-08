"""
Локальная LLM с использованием Ollama
"""

import requests
from typing import Optional
from config.settings import get_settings


class LocalLLM:
    """Интеграция с локальной LLM через Ollama"""

    def __init__(self, base_url: Optional[str] = None, model: Optional[str] = None):
        settings = get_settings()
        self.base_url = base_url or settings.ollama_base_url
        self.model = model or settings.ollama_model

    def generate(self, prompt: str, temperature: float = 0.7) -> str:
        """
        Генерировать текст на основе промпта

        Args:
            prompt: входной текст
            temperature: параметр креативности

        Returns:
            сгенерированный текст
        """
        try:
            response = requests.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "temperature": temperature,
                    "stream": False,
                },
                timeout=60
            )
            response.raise_for_status()
            return response.json().get("response", "")
        except Exception as e:
            return f"Error generating response: {str(e)}"

    def is_available(self) -> bool:
        """Проверить доступность LLM"""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=5)
            return response.status_code == 200
        except Exception:
            return False
