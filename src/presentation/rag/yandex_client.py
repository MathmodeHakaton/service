"""
Тонкая обёртка над Yandex AI Studio через OpenAI-совместимый endpoint.

Yandex выдаёт два совместимых интерфейса:
    • Responses API   : client.responses.create(...) — для single-shot ответов.
    • Chat Completions: client.chat.completions.create(...) — для multi-turn чата.

Здесь используем chat.completions — он естественно описывает диалог
с историей сообщений (role/content). Single-shot тоже укладывается:
просто одно сообщение пользователя + system instruction.

Аутентификация:
    api_key  ← settings.yandex_api_key   (Api-Key из Yandex Cloud)
    project  ← settings.yandex_folder_id (Folder ID)
    model    ← gpt://{folder}/{name}/{version}, например
               gpt://b1g6.../yandexgpt-5-lite/latest
"""
from __future__ import annotations

from functools import lru_cache
from typing import List, Dict

from openai import OpenAI

from config.settings import get_settings


@lru_cache(maxsize=1)
def get_client() -> OpenAI:
    s = get_settings()
    if not s.yandex_api_key or not s.yandex_folder_id:
        raise RuntimeError(
            "Не заданы YANDEX_API_KEY и YANDEX_FOLDER_ID в окружении/.env"
        )
    return OpenAI(
        api_key=s.yandex_api_key,
        base_url=s.yandex_base_url,
        project=s.yandex_folder_id,
    )


def model_uri(model: str | None = None) -> str:
    """Строит gpt://{folder}/{name}/{version} из settings."""
    s = get_settings()
    return f"gpt://{s.yandex_folder_id}/{model or s.yandex_model_chat}"


def complete(system_text: str, user_text: str, *,
             model: str | None = None,
             temperature: float = 0.0,
             max_tokens: int = 700,
             timeout: float = 60.0) -> str:
    """Single-shot: одно system + одно user сообщение. Возвращает текст ответа."""
    client = get_client()
    resp = client.chat.completions.create(
        model=model_uri(model),
        messages=[
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_text},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )
    return (resp.choices[0].message.content or "").strip()


def chat(messages: List[Dict[str, str]], system_text: str, *,
         model: str | None = None,
         temperature: float = 0.0,
         max_tokens: int = 800,
         timeout: float = 60.0) -> str:
    """
    Multi-turn чат.
    messages: [{'role':'user'|'assistant', 'content':'...'}] — история диалога.
    system_text: уже собранный system-промпт (с RAG-контекстом).
    """
    client = get_client()
    payload = [{"role": "system", "content": system_text}] + [
        {"role": m["role"], "content": m["content"]} for m in messages
    ]
    resp = client.chat.completions.create(
        model=model_uri(model),
        messages=payload,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )
    return (resp.choices[0].message.content or "").strip()
