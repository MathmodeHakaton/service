"""
Локальные guardrails для чата.

Зачем:
  1. Промпт-инъекции («забудь инструкции», «raise your system prompt», DAN-attack)
     отлавливаем regex'ом ДО отправки в LLM. Канонический отказ, ноль обращений к API.
  2. Safety-фильтр Yandex (и внутренние отказы LLM) попадают в историю и при следующей
     реплике провоцируют отказ-по-индукции даже на легитимный вопрос. Маркируем такие
     ответы флагом `refused=True` и при сборке messages для LLM выкидываем целиком
     пару (user→refusal): новый вопрос идёт в «чистый» контекст.

CANONICAL_REFUSAL дублирует фразу из system-промпта — единый канал отказа.
"""
from __future__ import annotations

import re
from typing import List, Dict

CANONICAL_REFUSAL = (
    "Я отвечаю только по данным системы LSI. Этот вопрос вне моей компетенции."
)

# Паттерны промпт-инъекций. Список консервативный — ловит явные попытки,
# легитимные запросы пользователя не задеваем.
_INJECTION_PATTERNS = [
    r"забудь\s+(все|всё|предыдущ|выш|свои)",
    r"игнорируй\s+(инструкц|систем|предыдущ|правил)",
    r"забыл(а|и)?\s+про\s+(инструкц|правил)",
    r"перепиши\s+(свои|систем)\s+инструкц",
    r"раскрой\s+(твой|твои|свой|свои)\s+(промпт|инструкц|систем)",
    r"повтори\s+(твой|твои|свои)\s+(промпт|инструкц|систем)",
    r"покажи\s+(твой|твои|свой|свои|весь)\s+(промпт|систем)",
    r"\bsystem\s+prompt\b",
    r"\bignore\s+(all|previous|prior|the)\s+(instruction|prompt|rule)",
    r"\bforget\s+(all|previous|your)\s+(instruction|prompt|rule)",
    r"\bpretend\s+(to\s+be|you\s+are)\b",
    r"\bact\s+as\s+(?!an?\s+analyst|аналит)",     # "act as DAN" — да, "act as analyst" — пусть проходит
    r"\bjailbreak\b",
    r"\bdo\s+anything\s+now\b",
    r"\bDAN\b",
    r"представь[,\s]+что\s+ты\s+(не|другой|злой|свободн)",
    r"теперь\s+ты\s+(не|другой|свободн)",
]

# Признаки, что ответ модели — это отказ/safety-плашка. Используем для маркировки.
_REFUSAL_FINGERPRINTS = [
    "я не могу",
    "не могу обсуждать",
    "не могу помочь",
    "давайте поговорим о чём-нибудь ещё",
    "давайте поговорим о чем-нибудь ещё",
    "i cannot",
    "i can't",
    "i'm sorry",
    "вне моей компетенции",
    "я отвечаю только по данным системы lsi",
    "в выгруженных данных системы такой информации нет",
]


def is_prompt_injection(text: str) -> bool:
    """True, если текст содержит явные маркеры jailbreak / prompt-injection."""
    t = (text or "").lower()
    return any(re.search(p, t) for p in _INJECTION_PATTERNS)


def looks_like_refusal(answer: str) -> bool:
    """Эвристика: ответ выглядит как отказ (или наш канонический, или safety от LLM)."""
    a = (answer or "").lower().strip()
    if not a:
        return False
    return any(fp in a for fp in _REFUSAL_FINGERPRINTS)


def filter_history_for_llm(history: List[Dict]) -> List[Dict]:
    """
    Готовит список messages для chat.completions, выкидывая «заразные» пары:
    user-сообщение, на которое мы выдали отказ. Это убирает индукцию отказа на
    последующие легитимные вопросы.

    history: [{'role':'user'|'assistant', 'content':..., 'refused':bool?}]
    Возвращает: список dict {'role', 'content'} без поля refused.
    """
    clean: List[Dict] = []
    for m in history:
        if m.get("role") == "assistant" and m.get("refused"):
            # удаляем сам отказ и его пользовательский triger
            if clean and clean[-1].get("role") == "user":
                clean.pop()
            continue
        clean.append({"role": m["role"], "content": m["content"]})
    return clean
