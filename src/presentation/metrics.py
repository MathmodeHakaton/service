"""
Метрики качества системы — три значимых семейства.

1. model_regression_metrics()  — точность регрессии CatBoost (Holdout/CV MAE).
2. crisis_recall()              — детекция исторических кризисов из ТЗ.
3. retriever_metrics()          — Recall@k + MRR + Hit@k + avg_rank + fails
                                  на 12 эталонных запросах казначея.

Метрики 3 (sensitivity ±20%) и 5 (data coverage) из прежней версии исключены —
первая писалась лишь частично в metadata и плохо отражала реальное качество,
вторая описывала структурное ограничение источников, а не качество системы.

Метрики 1 и 2 пересчитываются при retrain (`run_pipeline.py`).
Метрика 3 — на лету, по GOLDEN_SET.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import joblib
import pandas as pd

from src.presentation.rag.retriever import retrieve
from src.presentation.rag.knowledge_base import Chunk
from src.presentation.rag.golden_set import GOLDEN_SET, GoldenItem


ART = Path(__file__).resolve().parents[2] / "data" / "model_artifacts"


# ══════════════════════════════════════════════════════════════════════
# 1. РЕГРЕССИЯ
# ══════════════════════════════════════════════════════════════════════

@dataclass
class RegressionMetrics:
    holdout_mae: float
    cv_mae_mean: float
    cv_mae_std: float
    best_iteration: int
    train_rows: int

    def grade(self) -> str:
        if self.holdout_mae < 8:
            return "🟢 отлично"
        if self.holdout_mae < 15:
            return "🟡 приемлемо"
        return "🔴 высокая ошибка"


def model_regression_metrics() -> RegressionMetrics | None:
    meta_path = ART / "lsi_ml_metadata.joblib"
    if not meta_path.exists():
        return None
    meta = joblib.load(meta_path)
    m = meta.get("metrics", {})
    return RegressionMetrics(
        holdout_mae=float(m.get("holdout_mae", float("nan"))),
        cv_mae_mean=float(m.get("cv_mae_mean", float("nan"))),
        cv_mae_std=float(m.get("cv_mae_std", float("nan"))),
        best_iteration=int(m.get("best_iteration", 0)),
        train_rows=int(m.get("train_rows", 0)),
    )


# ══════════════════════════════════════════════════════════════════════
# 2. CRISIS RECALL
# ══════════════════════════════════════════════════════════════════════

@dataclass
class CrisisMetrics:
    episodes: pd.DataFrame
    recall: float
    mean_share_red: float
    n_episodes: int

    def grade(self) -> str:
        if self.recall >= 1.0 and self.mean_share_red >= 0.15:
            return "🟢 все кризисы пойманы"
        if self.recall >= 0.5:
            return "🟡 частично"
        return "🔴 пропущены"


def crisis_recall() -> CrisisMetrics | None:
    p = ART / "backtest_crisis_episodes.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p)
    if df.empty:
        return None
    ok = df["verdict"].astype(str).str.startswith("OK").sum()
    return CrisisMetrics(
        episodes=df,
        recall=float(ok) / len(df),
        mean_share_red=float(pd.to_numeric(df["share_red"], errors="coerce").mean()),
        n_episodes=len(df),
    )


# ══════════════════════════════════════════════════════════════════════
# 3. RETRIEVER
# ══════════════════════════════════════════════════════════════════════

@dataclass
class RetrieverItemResult:
    query: str
    retrieved_titles: List[str]
    expected: List[str]
    hit: bool
    first_rank: int
    recall_at_k: float


@dataclass
class RetrieverMetrics:
    k: int
    recall_at_k: float
    mrr: float
    hit_rate: float
    avg_first_rank: float          # средний ранг ПЕРВОГО релевантного среди хитов
    fail_queries: List[str]        # запросы, на которых retriever промахнулся целиком
    items: List[RetrieverItemResult]

    def grade(self) -> str:
        if self.recall_at_k >= 0.9 and self.mrr >= 0.75:
            return "🟢 точный"
        if self.recall_at_k >= 0.7 and self.hit_rate >= 0.85:
            return "🟡 приемлемый"
        return "🔴 промахи"


def _is_relevant(chunk: Chunk, item: GoldenItem) -> bool:
    if chunk.id in item.relevant_ids:
        return True
    if item.relevant_tags and (item.relevant_tags & chunk.tags):
        return True
    return False


def retriever_metrics(chunks: List[Chunk], k: int = 6) -> RetrieverMetrics:
    recalls, mrrs, hits = [], [], []
    ranks_on_hit: List[int] = []
    fails: List[str] = []
    items: List[RetrieverItemResult] = []
    for gi in GOLDEN_SET:
        top = retrieve(gi.query, chunks, k=k)
        rel_flags = [_is_relevant(c, gi) for c in top]
        # Recall по совпадению id или тега
        n_relevant_total = max(1, len(gi.relevant_ids))
        n_relevant_found = sum(1 for c in top if _is_relevant(c, gi))
        recall = min(1.0, n_relevant_found / n_relevant_total)
        first_rank = next((i + 1 for i, r in enumerate(rel_flags) if r), 0)
        mrr = (1.0 / first_rank) if first_rank > 0 else 0.0
        hit = first_rank > 0

        if hit:
            ranks_on_hit.append(first_rank)
        else:
            fails.append(gi.query)

        items.append(RetrieverItemResult(
            query=gi.query,
            retrieved_titles=[c.title for c in top],
            expected=sorted(gi.relevant_ids | gi.relevant_tags),
            hit=hit, first_rank=first_rank, recall_at_k=recall,
        ))
        recalls.append(recall); mrrs.append(mrr); hits.append(1.0 if hit else 0.0)

    n = max(1, len(GOLDEN_SET))
    return RetrieverMetrics(
        k=k,
        recall_at_k=sum(recalls) / n,
        mrr=sum(mrrs) / n,
        hit_rate=sum(hits) / n,
        avg_first_rank=(sum(ranks_on_hit) / len(ranks_on_hit)) if ranks_on_hit else 0.0,
        fail_queries=fails,
        items=items,
    )
