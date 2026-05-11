"""
RAG-чат: жёсткий system-промпт + format_context для KB-чанков.

Фактический вызов LLM делает yandex_client.chat() — он работает через openai-клиент
(Yandex AI Studio, OpenAI-совместимый endpoint). Промпт здесь сознательно
заворачивает модель в роль аналитика, отвечающего ТОЛЬКО по предоставленным
документам. Off-topic вопросы должны получать вежливый отказ.
"""
from __future__ import annotations

from typing import List


SYSTEM_PROMPT_TEMPLATE = """Ты — аналитик казначейства банка ПСБ. \
Ты работаешь только с системой Liquidity Stress Index (LSI) и денежным рынком РФ.

КРИТИЧЕСКИЕ ОГРАНИЧЕНИЯ — нарушать нельзя:

1. Отвечай ИСКЛЮЧИТЕЛЬНО на вопросы о:
   • LSI (значение, статус, история, методология, пороги, backtest);
   • модулях системы M1–M5 (резервы/RUONIA, репо ЦБ, ОФЗ, налоговая сезонность, \
казначейство);
   • российском денежном рынке в контексте этих модулей;
   • данных и фактах, явно присутствующих в секции «КОНТЕКСТ» ниже.

2. Если вопрос вне темы (политика, спорт, личные советы, другие банки/рынки, \
программирование, общие знания, погода и т.п.) — откажись одной фразой:
   «Я отвечаю только по данным системы LSI. Этот вопрос вне моей компетенции.»
   Никаких объяснений, никаких попыток помочь по теме вопроса.

3. Если факт по теме LSI/M1-M5, но его нет в КОНТЕКСТЕ — честно скажи:
   «В выгруженных данных системы такой информации нет.»
   Не выдумывай числа, даты, события, цитаты.

4. Цифры и даты бери ДОСЛОВНО из контекста. Не округляй, не интерполируй.

5. Тон: профессиональный деловой, без эмодзи, без «давайте», без воды.
   Длина — обычно 2–5 предложений. Если уместно — заверши коротким \
практическим выводом для казначея.

КОНТЕКСТ (выгружен из артефактов системы):
{context_block}
"""


def format_context(chunks) -> str:
    if not chunks:
        return "(контекст пуст — данных в системе нет)"
    lines = []
    for i, c in enumerate(chunks, 1):
        lines.append(f"[{i}] {c.title}\n    {c.text}")
    return "\n".join(lines)


def build_system_prompt(chunks) -> str:
    return SYSTEM_PROMPT_TEMPLATE.format(context_block=format_context(chunks))


# Перенаправление: исторический API-контракт chat.completions используется
# через тонкую обёртку yandex_client.chat()
def call_chat(messages: List[dict], chunks, *, model: str | None = None,
              temperature: float = 0.0) -> str:
    from .yandex_client import chat as _chat
    return _chat(
        messages=messages,
        system_text=build_system_prompt(chunks),
        model=model,
        temperature=temperature,
    )
