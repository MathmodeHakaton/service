"""
Query rewriting для ссылочных и слишком коротких follow-up'ов.

Когда пользователь пишет «а почему?», «там же», «в марте?», «а в 2023?» —
текущее сообщение почти не содержит признаков для retriever'а. Контекст
прячется в предыдущей реплике. Чтобы retriever нашёл правильные чанки,
мы переписываем запрос в самодостаточный, используя ОДИН вызов LLM.

Логика:
    • Если в новом запросе ≥ 4 нормализованных токена и хотя бы одна сущность
      (год, модуль, ключевое слово) — оставляем как есть, LLM не дёргаем.
    • Иначе берём последние 1-2 user-сообщения + текущее, просим LLM собрать
      одно ёмкое поисковое предложение. yandexgpt-5-lite, temperature=0,
      max_tokens=60.
    • При любом сбое — возвращаем исходный запрос (degrade gracefully).
"""
from __future__ import annotations

import re
from typing import List

from .text_norm import tokenize, extract_entities


_REWRITE_SYS = (
    "Ты — переписчик пользовательских запросов в системе анализа ликвидности. "
    "Тебе дают историю короткого диалога и последнее ссылочное сообщение "
    "(например: «а почему?», «в марте?», «и что с этим?»). "
    "Перепиши последнее сообщение пользователя как ОДНО самостоятельное "
    "поисковое предложение, явно подставив все упомянутые сущности из "
    "истории (год, месяц, модуль, термин). "
    "ВАЖНО: верни только переписанный запрос одной строкой без кавычек "
    "и без пояснений. Если запрос уже самодостаточен — верни его как есть."
)

_PRONOUN_RE = re.compile(
    r"\b(этот|эта|это|эти|тот|та|те|он|она|они|оно|его|её|их|"
    r"там|тогда|здесь|сейчас|так)\b",
    re.IGNORECASE | re.UNICODE,
)


def needs_rewrite(query: str) -> bool:
    """Эвристика: нужно ли переписывать. Без LLM-вызова."""
    if not query or len(query.strip()) < 3:
        return True
    toks = tokenize(query)
    ents = extract_entities(query)
    # Совсем короткий
    if len(toks) < 4:
        return True
    # Содержит ссылочные местоимения и при этом сущностей мало
    if _PRONOUN_RE.search(query) and len(ents) <= 1:
        return True
    return False


def rewrite_query(query: str, recent_user_turns: List[str]) -> str:
    """Запрашивает LLM переписать query, опираясь на N последних реплик
    пользователя. recent_user_turns — НЕ включая текущий query.

    Если history пустая или LLM упал — возвращаем оригинал.
    """
    if not recent_user_turns:
        return query
    try:
        from .yandex_client import complete
    except Exception:
        return query

    # Собираем компактный user-prompt
    lines = ["История запросов пользователя (от старых к новому):"]
    for i, t in enumerate(recent_user_turns[-3:], 1):
        lines.append(f"{i}. {t}")
    lines.append(f"Последний ссылочный запрос: «{query}»")
    lines.append("Перепиши последний запрос в одну самостоятельную фразу.")
    user_text = "\n".join(lines)

    try:
        out = complete(
            system_text=_REWRITE_SYS,
            user_text=user_text,
            model=None,                  # default yandex_model_chat (lite)
            temperature=0.0,
            max_tokens=60,
            timeout=15.0,
        )
    except Exception:
        return query

    # Подчищаем артефакты типа «Запрос:» / кавычки
    out = out.strip().strip("«»\"' ").lstrip("Запрос:").lstrip("Перепис").strip(": ")
    return out or query
