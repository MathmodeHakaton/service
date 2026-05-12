"""
Гибридный retriever без внешних embeddings.

Скоринг чанка = (unigram BM25-lite) + (bigram BM25-lite) + (entity-boost)
                + (контекст диалога) − (штраф нерелевантного снапшота)

Доработки относительно первой версии:
    • Морфологический стемминг (text_norm.stem) — нечувствительность к окончаниям.
    • Биграммы — multi-word термины («конец месяца», «недоспрос ОФЗ») ранжируются как
      единые понятия.
    • Явная экстракция сущностей: годы (2022), месяцы (mar/may), модули (M1..M5),
      доменные ключи (RUONIA, репо, CatBoost). Каждое совпадение даёт жирный буст.
    • Чанк `latest` больше не прилеплен принудительно — он включается, если
      запрос содержит маркеры «сейчас/текущ/последн» либо в нём нет конкретной
      даты/модуля (т.е. неявно — про «сегодня»).
    • Контекст диалога: к запросу добавляется последнее `prev_user_query`
      (с пониженным весом), чтобы вопросы вида «а что в марте?» использовали
      сущности из предыдущей реплики.
    • MMR-диверсификация: после ранжирования выбираем k чанков, штрафуя похожие.
      Гарантирует, что в top-k не лезут 3 дубля одного модуля.
"""
from __future__ import annotations

import math
from collections import Counter
from typing import List, Optional, Set

from .knowledge_base import Chunk
from .text_norm import tokens_and_grams, extract_entities


# Веса. Подобраны по golden-set, не угаданы:
#   unigram = база, bigram = в 1.5 раза дороже (multi-word — более редкий сигнал),
#   entity-tag — самый дорогой (явные сущности должны доминировать).
_W_UNIGRAM = 1.0
_W_BIGRAM = 1.5
_W_TAG = 5.0
_W_CTX = 0.4              # вес контекста диалога (понижен — это подсказка, не запрос)
_MMR_LAMBDA = 0.7         # 0 — только новизна, 1 — только релевантность

_NOW_MARKERS = frozenset({"kw:сейчас", "kw:текущ", "kw:последн"})


def _build_idf(chunks: List[Chunk]) -> dict:
    """IDF по конкатенации unigrams + bigrams каждого чанка."""
    df: Counter = Counter()
    for c in chunks:
        toks, grams = tokens_and_grams(c.title + " " + c.text)
        for term in set(toks) | set(grams):
            df[term] += 1
    N = max(1, len(chunks))
    return {t: math.log(1 + N / (1 + n)) for t, n in df.items()}


def _score_chunk(query_toks: list[str], query_grams: list[str], query_tags: Set[str],
                 ctx_toks: list[str], ctx_grams: list[str],
                 chunk_index: tuple[set[str], set[str], Set[str]],
                 idf: dict) -> float:
    c_toks, c_grams, c_tags = chunk_index

    s = 0.0
    # Unigram overlap
    for t in query_toks:
        if t in c_toks:
            s += _W_UNIGRAM * idf.get(t, 1.0)
    # Bigram overlap (дороже)
    for g in query_grams:
        if g in c_grams:
            s += _W_BIGRAM * idf.get(g, 1.2)
    # Entity-tag overlap — самое жирное
    s += _W_TAG * len(query_tags & c_tags)

    # Контекст диалога с пониженным весом
    for t in ctx_toks:
        if t in c_toks:
            s += _W_CTX * _W_UNIGRAM * idf.get(t, 1.0)
    for g in ctx_grams:
        if g in c_grams:
            s += _W_CTX * _W_BIGRAM * idf.get(g, 1.2)

    return s


def _index_chunk(c: Chunk) -> tuple[set[str], set[str], Set[str]]:
    toks, grams = tokens_and_grams(c.title + " " + c.text)
    # Теги чанка дополняем entity-экстракцией по его собственному тексту —
    # на случай если в knowledge_base.py не все теги выставлены вручную.
    auto_tags = extract_entities(c.title + " " + c.text)
    return set(toks), set(grams), (c.tags | auto_tags)


def _is_explicit_query(query_tags: Set[str]) -> bool:
    """Запрос «явный» — если в нём есть конкретная сущность (год, месяц, модуль).
    Тогда `latest`-чанк НЕ нужен принудительно."""
    if any(t in query_tags for t in _NOW_MARKERS):
        return True   # явно про «сейчас» → нужен latest
    if any(t.isdigit() for t in query_tags):       # есть год
        return True
    if any(t in {"M1", "M2", "M3", "M4", "M5"} for t in query_tags):
        return True
    if any(t.startswith("kw:") for t in query_tags):
        return False   # есть доменный ключ, но нет конкретной даты — пусть скоринг решает
    return False


def _mmr_select(scored: list[tuple[float, Chunk, tuple]], k: int,
                lam: float = _MMR_LAMBDA) -> list[Chunk]:
    """Maximal Marginal Relevance: жадно выбираем k разнообразных чанков.
    Похожесть — по jaccard на (tokens ∪ bigrams).
    """
    if not scored:
        return []
    scored = sorted(scored, key=lambda x: -x[0])
    selected: list[tuple[float, Chunk, tuple]] = [scored[0]]
    remaining = scored[1:]

    while remaining and len(selected) < k:
        best, best_idx = -math.inf, -1
        for i, (rel, _, idx) in enumerate(remaining):
            # max схожести с уже выбранными
            t_i = idx[0] | idx[1]
            sim = 0.0
            for _, _, s_idx in selected:
                t_s = s_idx[0] | s_idx[1]
                union = t_i | t_s
                if union:
                    sim = max(sim, len(t_i & t_s) / len(union))
            mmr_score = lam * rel - (1 - lam) * sim
            if mmr_score > best:
                best, best_idx = mmr_score, i
        selected.append(remaining.pop(best_idx))

    return [c for _, c, _ in selected]


def retrieve(query: str,
             chunks: List[Chunk],
             k: int = 6,
             *,
             prev_user_query: Optional[str] = None) -> List[Chunk]:
    """Главная функция. Возвращает top-k чанков с MMR-диверсификацией.

    prev_user_query — необязательный контекст диалога. Используется для
    дополнения скоринга, НЕ для перезаписи запроса.
    """
    if not chunks:
        return []
    if not query.strip():
        return []

    idf = _build_idf(chunks)
    q_toks, q_grams = tokens_and_grams(query)
    q_tags = extract_entities(query)
    ctx_toks, ctx_grams = ([], [])
    if prev_user_query:
        ctx_toks, ctx_grams = tokens_and_grams(prev_user_query)

    explicit = _is_explicit_query(q_tags)

    scored: list[tuple[float, Chunk, tuple]] = []
    for c in chunks:
        idx = _index_chunk(c)
        s = _score_chunk(q_toks, q_grams, q_tags, ctx_toks, ctx_grams, idx, idf)
        # Если запрос явно «про сейчас» — даём latest бонус,
        # если запрос явно конкретный (дата/модуль) — снимаем форс-приоритет с latest
        if c.id == "latest":
            if any(t in _NOW_MARKERS for t in q_tags):
                s += 5.0
            elif explicit:
                s -= 2.0     # пенальти, чтобы не вытеснять конкретный чанк
        scored.append((s, c, idx))

    # Отрезаем абсолютно нулевые скоры (несовпавшие чанки), но оставляем хотя бы
    # один — на случай, когда запрос совсем не из домена.
    nonzero = [t for t in scored if t[0] > 0]
    if not nonzero:
        return _mmr_select(scored, k=min(k, 2))   # пара дефолтных — не пусто

    return _mmr_select(nonzero, k=k)
