"""
LLM-комментарий для LSI дашборда.

Используется квантованный Qwen через Ollama (по умолчанию qwen2.5:3b-instruct,
~2 ГБ Q4). Ollama держит модель в памяти между вызовами — латентность одного
комментария обычно 2–5 секунд на CPU.

Поднятие:
    ollama serve                       # фоновый демон
    ollama pull qwen2.5:3b-instruct    # ~2 ГБ
В settings.ollama_model можно подменить на qwen2.5:7b-instruct (выше качество,
~5 ГБ, дольше).
"""
from __future__ import annotations

from typing import Dict, Tuple
import requests
import streamlit as st

try:
    from config.settings import get_settings
    _s = get_settings()
    OLLAMA_URL: str = getattr(_s, "ollama_base_url", "http://localhost:11434")
    LLM_MODEL: str = getattr(_s, "ollama_model", None) or "qwen2.5:3b-instruct"
except Exception:
    OLLAMA_URL = "http://localhost:11434"
    LLM_MODEL = "qwen2.5:3b-instruct"


PROMPT_TEMPLATE = (
    "Текущий LSI: {lsi_value:.1f} ({status})\n"
    "Вклад модулей: М1={m1:+.2f}, М2={m2:+.2f}, М3={m3:+.2f}, "
    "М4={m4:+.2f}, М5={m5:+.2f}\n"
    "Активные флаги: {active_flags}\n"
    "Ближайшие события: налоги — {upcoming_tax_dates}; "
    "аукционы ОФЗ — {upcoming_ofz_auctions}\n\n"
    "Задача: напиши аналитический комментарий для казначея ПСБ "
    "на 3–5 предложений.\n"
    "Структура:\n"
    "1. Что происходит с ликвидностью прямо сейчас.\n"
    "2. Какие модули создают основное давление и почему "
    "(используй знаки и величины вкладов SHAP).\n"
    "3. Чего ожидать в ближайшие 1–2 недели с учётом налоговых дат "
    "и аукционов.\n"
    "Тон: профессиональный, без воды, цифры/факты в приоритете. "
    "Язык: русский."
)


def build_prompt(*, lsi_value: float, status: str,
                 contributions: Dict[str, float],
                 active_flags: str,
                 upcoming_tax_dates: str,
                 upcoming_ofz_auctions: str) -> str:
    return PROMPT_TEMPLATE.format(
        lsi_value=lsi_value, status=status,
        m1=contributions.get("M1", 0.0),
        m2=contributions.get("M2", 0.0),
        m3=contributions.get("M3", 0.0),
        m4=contributions.get("M4", 0.0),
        m5=contributions.get("M5", 0.0),
        active_flags=active_flags,
        upcoming_tax_dates=upcoming_tax_dates,
        upcoming_ofz_auctions=upcoming_ofz_auctions,
    )


def llm_available() -> Tuple[bool, str]:
    """Проверяет, что Ollama запущена и нужная модель присутствует."""
    try:
        r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=3)
        r.raise_for_status()
        tags = {m.get("name", "") for m in r.json().get("models", [])}
        if not tags:
            return False, "Ollama запущена, но модели не установлены."
        # Совпадение либо точное, либо по префиксу до ':'
        base = LLM_MODEL.split(":")[0]
        if LLM_MODEL in tags or any(t.startswith(base) for t in tags):
            return True, "OK"
        return False, (f"Модель {LLM_MODEL} не установлена. "
                       f"Доступно: {', '.join(sorted(tags)) or '—'}.")
    except Exception as e:
        return False, f"Ollama недоступна по {OLLAMA_URL} ({e})."


@st.cache_data(ttl=600, show_spinner=False)
def generate_commentary(prompt: str, temperature: float = 0.3) -> str:
    """Генерирует комментарий через Ollama. Кэшируется на 10 минут."""
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": LLM_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": temperature, "num_predict": 350},
            },
            timeout=120,
        )
        r.raise_for_status()
        return r.json().get("response", "").strip() or "(пустой ответ модели)"
    except Exception as e:
        return f"Ошибка генерации: {e}"
