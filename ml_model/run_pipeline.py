"""PSB Liquidity Stress Index — end-to-end pipeline.

Architecture (matches ТЗ requirements):
    1. Statistical layer (src/features.py + src/stress_components.py) — MAD-normalised signals per module.
    2. ML aggregation layer (src/lsi_ml.py) — CatBoost trained on real ground truth from CBR
       bank-sector liquidity table. SHAP per-day decomposition. M4 applied as multiplier.
    3. Smoothing (Kalman 1D) + hysteresis status on the dashboard.
    4. Backtest on dec 2014, feb 2022, aug 2023 + sensitivity analysis (±20% feature perturbation).
"""
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from src.features import build_daily_features, make_backtest_summary, safe_to_csv
from src.stress_components import compute_stress_components
from src.lsi_ml import train_ml_lsi, ML_FEATURES, MODULE_FEATURES
from src.llm_commentator import generate_commentary, write_commentary_markdown
from src.plotting import plot_dashboard


def make_top_dates(scored: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    cols = ["date", "lsi", "lsi_smoothed", "lsi_raw", "status", "target_stress",
            "contribution_M1", "contribution_M2", "contribution_M3", "contribution_M4", "contribution_M5",
            "m4_multiplier", "active_market_modules_count", "crisis_window"]
    valid = scored[scored.get("full_model_valid", 1) == 1].copy()
    return valid.sort_values("lsi_smoothed", ascending=False)[[c for c in cols if c in valid.columns]].head(n)


def make_report(scored: pd.DataFrame, info: dict, out_dir: Path, horizon: int) -> None:
    metrics = info["metrics"]
    backtest = info["backtest"]
    feat_imp = info["feature_importance"]
    valid = scored[scored.get("full_model_valid", 1) == 1].copy()

    lines = []
    lines.append("# PSB Liquidity Stress Index — ML aggregation layer")
    lines.append("")
    lines.append("## Архитектура")
    lines.append("1. **Статистический слой**: MAD-нормализованные z-scores и флаги по 5 модулям ([src/features.py](src/features.py), [src/stress_components.py](src/stress_components.py)).")
    lines.append("2. **ML-крышка** ([src/lsi_ml.py](src/lsi_ml.py)): CatBoost учится на **реальной ground truth** из таблицы «Ликвидность банковского сектора» ЦБ (`m5_structural_balance_bln`).")
    lines.append(f"   - target = percentile_rank(max(0, −structural_balance)) * 100 с горизонтом раннего предупреждения {horizon} дней")
    lines.append("3. **M4 как мультипликатор** (требование ТЗ): final_LSI = clip(base_pred * Seasonal_Factor, 0, 100). M4 НЕ входит в фичи модели.")
    lines.append("4. **Интерпретация**: SHAP per-day → contribution_M1..M5 (литеральная декомпозиция LSI). M4 как отдельный множительный вклад.")
    lines.append("5. **Сглаживание**: Kalman 1D + hysteresis status (банковский стандарт).")
    lines.append("")
    lines.append("## Метрики модели")
    for k, v in metrics.items():
        if k.startswith("sensitivity_"):
            continue
        if isinstance(v, float):
            lines.append(f"- {k}: {v:.4f}")
        else:
            lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## Sensitivity ±20% (как требует ТЗ)")
    lines.append("Среднее смещение LSI при изменении фичей модуля на ±20%.")
    sens_rows = []
    for module in MODULE_FEATURES:
        row = {"module": module,
               "shift_+20%": round(metrics.get(f"sensitivity_{module}_+20%_mean_shift", 0.0), 2),
               "shift_-20%": round(metrics.get(f"sensitivity_{module}_-20%_mean_shift", 0.0), 2),
               "max_abs_shift_+20%": round(metrics.get(f"sensitivity_{module}_+20%_abs_max_shift", 0.0), 2)}
        sens_rows.append(row)
    lines.append(pd.DataFrame(sens_rows).to_markdown(index=False))
    lines.append("")
    lines.append("## Backtest на кризисных эпизодах (ТЗ)")
    lines.append(backtest.to_markdown(index=False))
    lines.append("")
    lines.append("## Global feature importance (mean |SHAP|)")
    lines.append(feat_imp.to_markdown(index=False))
    lines.append("")
    lines.append("## Top dates by LSI")
    lines.append(make_top_dates(scored, 12).to_markdown(index=False))
    lines.append("")
    lines.append("## Распределение статусов (valid days)")
    lines.append(valid["status"].value_counts(normalize=True).round(3).to_markdown())
    lines.append("")
    (out_dir / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=str, default="data")
    parser.add_argument("--out-dir", type=str, default="outputs")
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--early-warning-horizon", type=int, default=2,
                        help="Forward-looking horizon for the target (early warning).")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Statistical layer: daily features + stress components (MAD-normalised per module).
    features = build_daily_features(data_dir, start=args.start, end=args.end)
    safe_to_csv(features, out_dir / "daily_features.csv")
    components = compute_stress_components(features)
    safe_to_csv(components, out_dir / "stress_components.csv")

    # 2. ML aggregation cap.
    scored, model, info = train_ml_lsi(components, out_dir, early_warning_horizon=args.early_warning_horizon)

    # Save the main timeseries + a slim dashboard extract.
    dashboard_cols = [
        "date", "full_model_valid", "coverage_score",
        "lsi", "lsi_raw", "lsi_smoothed", "lsi_base", "m4_multiplier",
        "status", "status_raw",
        "target_stress", "lsi_baseline_expected",
        "contribution_M1", "contribution_M2", "contribution_M3", "contribution_M4", "contribution_M5",
        "active_market_modules_count", "crisis_window",
    ]
    safe_to_csv(scored, out_dir / "lsi_timeseries.csv")
    safe_to_csv(scored[[c for c in dashboard_cols if c in scored.columns]], out_dir / "lsi_dashboard_extract.csv")
    safe_to_csv(make_backtest_summary(scored, lsi_col="lsi_smoothed"), out_dir / "backtest_summary.csv")
    safe_to_csv(make_top_dates(scored, 25), out_dir / "top_lsi_dates.csv")

    make_report(scored, info, out_dir, args.early_warning_horizon)
    chart_paths = plot_dashboard(scored, out_dir / "charts")

    # Optional: Yandex AI Studio commentary. Skipped silently if credentials are not set.
    try:
        result = generate_commentary(scored)
        if result is not None:
            text, context, user_prompt = result
            write_commentary_markdown(text, context, user_prompt, out_dir / "llm_commentary.md")
            print(f"LLM commentary: outputs/llm_commentary.md (LSI={context['lsi']}, {context['status']})")
        else:
            print("LLM commentary: skipped (set YANDEX_API_KEY and YANDEX_FOLDER_ID to enable).")
    except Exception as exc:
        print(f"LLM commentary: failed ({exc.__class__.__name__}: {exc})")

    print(f"Done. Outputs in {out_dir.resolve()}")
    print(f"ML features: {len(ML_FEATURES)} ({list(MODULE_FEATURES)})")
    print(f"Rows: {len(scored)}, valid: {int(scored.get('full_model_valid', 1).sum())}")
    print(f"Holdout MAE: {info['metrics']['holdout_mae']:.2f}")
    print(f"CV MAE: {info['metrics']['cv_mae_mean']:.2f} ± {info['metrics']['cv_mae_std']:.2f}")
    print("Crisis backtest:")
    for _, row in info["backtest"].iterrows():
        print(f"  {row['episode']:<14} mean={row['mean_lsi']} max={row['max_lsi']} share_red={row['share_red']} -> {row['verdict']}")
    print(f"Charts: {[str(p.relative_to(out_dir)) for p in chart_paths]}")


if __name__ == "__main__":
    main()
