"""
Golden set для оценки retriever'а.

Каждый item — реалистичный вопрос казначея + множество тегов/id чанков,
которые ОБЯЗАНЫ оказаться в top-k retrieval. Не «возможно полезно»,
а именно «без этого ответ невозможен».

Метрики на основе set:
    • Recall@k = |retrieved ∩ relevant| / |relevant|
    • MRR     = 1 / rank первого relevant в retrieved (0 если ни одного)
    • Hit@k   = 1 если хотя бы один relevant попал в top-k, иначе 0

`relevant_ids` — id чанков из knowledge_base.py
`relevant_tag` — теги; чанк считается релевантным, если ЛЮБОЙ его тег ∈ relevant_tags
                (страховка от ребейзов id'шников при пересборке KB)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Set


@dataclass
class GoldenItem:
    query: str
    relevant_ids: Set[str] = field(default_factory=set)
    relevant_tags: Set[str] = field(default_factory=set)


GOLDEN_SET: list[GoldenItem] = [
    # ── Вопросы о текущем состоянии ─────────────────────────────────────
    GoldenItem(
        query="Какой LSI сейчас и что на него влияет?",
        relevant_ids={"latest"},
        relevant_tags={"сейчас", "текущ", "последн"},
    ),
    GoldenItem(
        query="Сколько LSI на сегодня?",
        relevant_ids={"latest"},
        relevant_tags={"сейчас", "текущ"},
    ),

    # ── Вопросы о модулях ───────────────────────────────────────────────
    GoldenItem(
        query="Что показывает модуль M3 по ОФЗ?",
        relevant_ids={"m3_doc"},
        relevant_tags={"M3", "ОФЗ", "недоспрос"},
    ),
    GoldenItem(
        query="Расскажи про репо ЦБ и cover ratio.",
        relevant_ids={"m2_doc"},
        relevant_tags={"M2", "репо", "cover"},
    ),
    GoldenItem(
        query="Что такое RUONIA и как она связана с резервами?",
        relevant_ids={"m1_doc"},
        relevant_tags={"M1", "RUONIA", "резервы"},
    ),
    GoldenItem(
        query="Как работает налоговый мультипликатор в системе?",
        relevant_ids={"m4_doc"},
        relevant_tags={"M4", "налоги", "сезонность", "мультипликатор"},
    ),
    GoldenItem(
        query="Что такое структурный баланс ликвидности?",
        relevant_ids={"m5_doc"},
        relevant_tags={"M5", "структурный", "баланс", "казначейство"},
    ),

    # ── Вопросы по истории ─────────────────────────────────────────────
    GoldenItem(
        query="Что происходило с ликвидностью в марте 2022 года?",
        relevant_ids={"yr_2022", "ep_feb_mar_2022"},
        relevant_tags={"2022", "кризис"},
    ),
    GoldenItem(
        query="Покажи стрессовый эпизод августа 2023.",
        relevant_ids={"yr_2023", "ep_aug_2023"},
        relevant_tags={"2023", "кризис", "aug_2023"},
    ),
    GoldenItem(
        query="Какие дни были самыми стрессовыми в истории?",
        relevant_ids={"top_days"},
        relevant_tags={"топ", "пик", "максимум"},
    ),

    # ── Методология ─────────────────────────────────────────────────────
    GoldenItem(
        query="Как устроена агрегация LSI и при чём тут SHAP?",
        relevant_ids={"lsi_method", "mod_imp"},
        relevant_tags={"SHAP", "CatBoost", "методология", "веса", "важность"},
    ),
    GoldenItem(
        query="Что значит красный статус и где порог?",
        relevant_ids={"thresholds"},
        relevant_tags={"статус", "порог", "красный"},
    ),
]
