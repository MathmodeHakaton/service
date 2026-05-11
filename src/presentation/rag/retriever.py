"""
Лёгкий гибридный retriever без внешних зависимостей.

Скоринг чанка = базовый token-overlap (BM25-lite: term coverage с лёгким IDF-весом)
                + бусты по тегам (год / модуль / ключевые слова) из запроса.

Чанк "Текущий снапшот" всегда включается первым (системный контекст).
Затем сортируем остальные по убыванию score и берём top-k.

Этого хватает на структурированный KB из ~30 коротких чанков. Если объём вырастет,
переедем на Yandex embeddings (text-search-doc/query) — API контракт совместим.
"""
from __future__ import annotations

import math
import re
from collections import Counter
from typing import List, Set

from .knowledge_base import Chunk

_TOKEN = re.compile(r"[\wа-яёА-ЯЁ]+", re.UNICODE)
_STOP = {
    "и", "в", "на", "по", "с", "для", "из", "от", "до", "что", "как", "это",
    "за", "под", "над", "при", "у", "о", "об", "же", "ли", "не", "ни",
    "а", "но", "или", "то", "если", "так", "тут", "вот", "был", "была", "было",
    "при", "при этом", "ведь", "the", "of", "in", "on", "is", "are", "to", "and",
    "что-то", "чтобы",
}
_MODULE_KEYS = {"м1": "M1", "m1": "M1", "м2": "M2", "m2": "M2",
                "м3": "M3", "m3": "M3", "м4": "M4", "m4": "M4",
                "м5": "M5", "m5": "M5"}


def _tokens(text: str) -> List[str]:
    return [t.lower() for t in _TOKEN.findall(text or "")
            if len(t) > 2 and t.lower() not in _STOP]


def _query_tags(query: str) -> Set[str]:
    """Извлекаем годы и упоминания модулей."""
    tags: Set[str] = set()
    for year in re.findall(r"(19|20)\d{2}", query):
        tags.add(query[query.find(year):query.find(year) + 4])
    for m in re.findall(r"\b(?:м|m)[1-5]\b", query.lower()):
        tags.add(_MODULE_KEYS[m])
    q_low = query.lower()
    for kw in ("ruonia", "руониа", "репо", "офз", "ofz", "налог", "квартал",
               "месяц", "ключевая", "корсчёт", "корсчет", "казначейство",
               "бюджет", "ликвидность", "стресс", "красн", "жёлт", "пик",
               "максимум", "недоспрос", "переспрос", "сейчас", "последн",
               "методолог", "shap", "catboost", "веса", "важность"):
        if kw in q_low:
            tags.add(kw)
    return tags


def _idf(chunks: List[Chunk]) -> dict:
    df = Counter()
    for c in chunks:
        for t in set(_tokens(c.text + " " + c.title)):
            df[t] += 1
    N = len(chunks) or 1
    return {t: math.log(1 + N / (1 + n)) for t, n in df.items()}


def retrieve(query: str, chunks: List[Chunk], k: int = 6) -> List[Chunk]:
    if not query.strip() or not chunks:
        return chunks[:k]

    idf = _idf(chunks)
    q_toks = _tokens(query)
    q_tag_set = _query_tags(query)

    def score(c: Chunk) -> float:
        c_toks = _tokens(c.text + " " + c.title)
        if not c_toks:
            return 0.0
        # token overlap, веса по IDF
        c_set = set(c_toks)
        s = sum(idf.get(t, 1.0) for t in q_toks if t in c_set)
        # буст по тегам
        tag_hits = q_tag_set & c.tags
        s += 4.0 * len(tag_hits)
        # лёгкий буст коротким чанкам (плотнее по факту)
        s *= 1.0 + 1.0 / (1.0 + math.log(1 + len(c_toks)))
        return s

    # Снапшот (id=latest) приклеиваем сверху — он почти всегда нужен.
    latest = next((c for c in chunks if c.id == "latest"), None)
    rest = [c for c in chunks if c.id != "latest"]
    rest.sort(key=score, reverse=True)
    out = ([latest] if latest else []) + rest[: max(0, k - (1 if latest else 0))]
    return out
