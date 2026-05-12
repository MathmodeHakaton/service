"""
Расчёт трёх семейств метрик качества и запись отчёта для презентации.

Источники:
    • Метрики модели и кризисов  — data/model_artifacts/ (создаются retrain'ом).
    • Retriever на golden-set    — рассчитывается на лету.

Артефакты:
    docs/METRICS_REPORT.md   — человекочитаемо.
    docs/metrics_snapshot.json — машиночитаемо.

Запуск:
    python scripts/compute_metrics.py
    python scripts/compute_metrics.py --k 6
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.presentation.metrics import (
    model_regression_metrics, crisis_recall, retriever_metrics,
)
from src.presentation.rag.knowledge_base import build_knowledge_base


def _fmt(v, fmt="{:.2f}") -> str:
    try:
        return fmt.format(float(v))
    except (TypeError, ValueError):
        return "—"


def build_report(k: int) -> tuple[str, dict]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    reg = model_regression_metrics()
    crisis = crisis_recall()
    chunks = build_knowledge_base()
    ret = retriever_metrics(chunks, k=k)

    snapshot = {
        "generated_at": now,
        "regression": (
            {
                "holdout_mae": reg.holdout_mae,
                "cv_mae_mean": reg.cv_mae_mean,
                "cv_mae_std": reg.cv_mae_std,
                "best_iteration": reg.best_iteration,
                "train_rows": reg.train_rows,
                "grade": reg.grade(),
            } if reg else None
        ),
        "crisis": (
            {
                "recall": crisis.recall,
                "mean_share_red": crisis.mean_share_red,
                "n_episodes": crisis.n_episodes,
                "episodes": crisis.episodes.to_dict(orient="records"),
                "grade": crisis.grade(),
            } if crisis else None
        ),
        "retriever": {
            "k": ret.k,
            "recall_at_k": ret.recall_at_k,
            "mrr": ret.mrr,
            "hit_rate": ret.hit_rate,
            "avg_first_rank": ret.avg_first_rank,
            "fail_queries": ret.fail_queries,
            "n_queries": len(ret.items),
            "grade": ret.grade(),
            "items": [
                {"query": it.query, "hit": it.hit, "first_rank": it.first_rank,
                 "recall_at_k": it.recall_at_k,
                 "retrieved_titles": it.retrieved_titles,
                 "expected": it.expected}
                for it in ret.items
            ],
        },
    }

    lines: list[str] = []
    lines.append("# Метрики качества системы LSI")
    lines.append("")
    lines.append(f"_Сгенерировано: {now}_")
    lines.append("")
    lines.append("Три значимых семейства: точность регрессии CatBoost, детекция "
                 "исторических кризисов из ТЗ, качество retriever'а на golden-set.")
    lines.append("")

    # Сводка
    lines.append("## Сводка")
    lines.append("")
    lines.append("| Метрика | Значение | Оценка |")
    lines.append("|---|---|---|")
    if reg:
        lines.append(f"| **1. Holdout MAE** | {reg.holdout_mae:.2f} "
                     f"(CV {reg.cv_mae_mean:.2f} ± {reg.cv_mae_std:.2f}) | {reg.grade()} |")
    else:
        lines.append("| 1. Holdout MAE | нет данных | — |")
    if crisis:
        lines.append(f"| **2. Crisis recall** | {crisis.recall:.0%} "
                     f"({crisis.n_episodes} эпизодов, средн. share_red {crisis.mean_share_red:.0%}) | {crisis.grade()} |")
    else:
        lines.append("| 2. Crisis recall | нет данных | — |")
    lines.append(f"| **3. Retriever Recall@{ret.k}** | "
                 f"{ret.recall_at_k:.0%} (MRR {ret.mrr:.2f}, Hit-rate {ret.hit_rate:.0%}, "
                 f"avg rank {ret.avg_first_rank:.2f}) | {ret.grade()} |")
    lines.append("")

    # ── 1. Регрессия ──────────────────────────────────────────────
    lines.append("## 1. Регрессия CatBoost")
    lines.append("")
    lines.append("Точность прогноза LSI на отложенной выборке (последние 15% дат) "
                 "и в 5-fold time-series кросс-валидации (без утечки будущего).")
    lines.append("")
    if reg:
        lines.append("| Параметр | Значение |")
        lines.append("|---|---|")
        lines.append(f"| Holdout MAE | {reg.holdout_mae:.2f} |")
        lines.append(f"| CV MAE (mean) | {reg.cv_mae_mean:.2f} |")
        lines.append(f"| CV MAE (std) | {reg.cv_mae_std:.2f} |")
        lines.append(f"| Best iteration | {reg.best_iteration} |")
        lines.append(f"| Train rows | {reg.train_rows} |")
        lines.append("")
        lines.append(f"Оценка: **{reg.grade()}** "
                     "(<8 — отлично, <15 — приемлемо, ≥15 — высокая ошибка).")
    else:
        lines.append("_Нет `lsi_ml_metadata.joblib`. Запустите retrain._")
    lines.append("")

    # ── 2. Crisis recall ──────────────────────────────────────────
    lines.append("## 2. Детекция исторических кризисов")
    lines.append("")
    lines.append("Backtest на стресс-эпизодах из ТЗ. Эпизод засчитывается как пойманный "
                 "(`verdict = OK_red_reached`), если в его окне хотя бы один день "
                 "LSI ≥ 70 (красная зона).")
    lines.append("")
    if crisis:
        lines.append("| Эпизод | Период | Дней | Средний LSI | Макс LSI | Share red | Verdict |")
        lines.append("|---|---|---|---|---|---|---|")
        for _, r in crisis.episodes.iterrows():
            lines.append(
                f"| {r['episode']} | {r['start']} → {r['end']} | {r['n_days']} | "
                f"{_fmt(r['mean_lsi'])} | {_fmt(r['max_lsi'])} | "
                f"{_fmt(r['share_red'])} | {r['verdict']} |"
            )
        lines.append("")
        lines.append(f"Оценка: **{crisis.grade()}** · recall = {crisis.recall:.0%}, "
                     f"средняя доля красных дней внутри эпизодов = {crisis.mean_share_red:.0%}.")
    else:
        lines.append("_Нет `backtest_crisis_episodes.csv`._")
    lines.append("")

    # ── 3. Retriever ──────────────────────────────────────────────
    lines.append("## 3. Retriever на golden-set")
    lines.append("")
    lines.append(f"Golden-set: {len(ret.items)} типовых вопросов казначея, для каждого "
                 f"вручную зафиксирован набор обязательных чанков (relevant_ids/tags). "
                 f"Retriever на этих запросах должен выдавать релевантные в top-{ret.k}.")
    lines.append("")
    lines.append("| Метрика | Значение | Смысл |")
    lines.append("|---|---|---|")
    lines.append(f"| Recall@{ret.k} | **{ret.recall_at_k:.0%}** | доля релевантных в top-k |")
    lines.append(f"| MRR | **{ret.mrr:.2f}** | средний обратный ранг первого релевантного |")
    lines.append(f"| Hit-rate | **{ret.hit_rate:.0%}** | доля запросов, где найден хоть один |")
    lines.append(f"| Avg first rank | **{ret.avg_first_rank:.2f}** | средняя позиция первого релевантного среди хитов |")
    lines.append(f"| Fails | **{len(ret.fail_queries)}** | запросов с полным промахом |")
    lines.append("")
    lines.append(f"Оценка: **{ret.grade()}** "
                 "(Recall@k ≥ 90% и MRR ≥ 0.75 — точный; Recall ≥ 70% и Hit ≥ 85% — приемлемый).")
    if ret.fail_queries:
        lines.append("")
        lines.append("**Запросы с промахом:**")
        for q in ret.fail_queries:
            lines.append(f"- {q}")
    lines.append("")
    lines.append("### Разбивка по запросам")
    lines.append("")
    lines.append("| # | Запрос | Hit | Первый ранг | Recall@k |")
    lines.append("|---|---|---|---|---|")
    for i, it in enumerate(ret.items, 1):
        rank = it.first_rank if it.first_rank else "—"
        hit = "✅" if it.hit else "❌"
        lines.append(f"| {i} | {it.query} | {hit} | {rank} | {it.recall_at_k:.2f} |")
    lines.append("")

    return "\n".join(lines), snapshot


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--k", type=int, default=6)
    ap.add_argument("--out-md", type=str,
                    default=str(ROOT / "docs" / "METRICS_REPORT.md"))
    ap.add_argument("--out-json", type=str,
                    default=str(ROOT / "docs" / "metrics_snapshot.json"))
    args = ap.parse_args()

    md, snap = build_report(k=args.k)

    out_md = Path(args.out_md)
    out_json = Path(args.out_json)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(md, encoding="utf-8")
    out_json.write_text(json.dumps(snap, ensure_ascii=False, indent=2, default=str),
                        encoding="utf-8")

    print(f"Saved: {out_md.relative_to(ROOT)}")
    print(f"Saved: {out_json.relative_to(ROOT)}")
    print()
    print("Сводка:")
    if snap["regression"]:
        print(f"  1. Holdout MAE       = {snap['regression']['holdout_mae']:.2f}  "
              f"({snap['regression']['grade']})")
    if snap["crisis"]:
        print(f"  2. Crisis recall     = {snap['crisis']['recall']:.0%}    "
              f"({snap['crisis']['grade']})")
    r = snap["retriever"]
    print(f"  3. Retriever Recall@{r['k']} = {r['recall_at_k']:.0%}, MRR = {r['mrr']:.2f}, "
          f"avg rank = {r['avg_first_rank']:.2f}  ({r['grade']})")
    if r["fail_queries"]:
        print(f"     fails: {len(r['fail_queries'])} - {r['fail_queries']}")


if __name__ == "__main__":
    main()
