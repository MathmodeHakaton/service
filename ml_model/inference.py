"""
LSI inference-only: грузит обученную CatBoost-модель и пересчитывает LSI на
свежих данных, БЕЗ повторного fit.

Сценарий: каждый день парсеры обновляют ml_model/data/*.csv → этот скрипт
строит фичи, делает predict + SHAP, применяет M4-мультипликатор, сглаживает
Калманом и пишет те же артефакты, что и run_pipeline.py. На CPU работает
секунды, не минуты.

Полный retrain (раз в неделю) делает run_pipeline.py.

Запуск:
    python inference.py --data-dir data --out-dir outputs
                        [--model-dir <path with lsi_ml_model.cbm>]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from catboost import CatBoostRegressor, Pool

from src.features import build_daily_features, safe_to_csv
from src.stress_components import compute_stress_components
from src.lsi_ml import (
    ML_FEATURES,
    FEATURE_TO_MODULE,
    build_indicator_signals,
    kalman_1d_smooth,
    hysteresis_status,
    _backtest_crisis,
)


DASHBOARD_COLS = [
    "date", "full_model_valid", "coverage_score",
    "lsi", "lsi_raw", "lsi_smoothed", "lsi_base", "m4_multiplier",
    "status", "status_raw",
    "lsi_baseline_expected",
    "contribution_M1", "contribution_M2", "contribution_M3",
    "contribution_M4", "contribution_M5",
    "active_market_modules_count", "crisis_window",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=str, default="data")
    ap.add_argument("--out-dir", type=str, default="outputs")
    ap.add_argument("--model-dir", type=str, default=None,
                    help="Путь, где лежит lsi_ml_model.cbm и lsi_ml_metadata.joblib. "
                         "По умолчанию = --out-dir.")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    model_dir = Path(args.model_dir) if args.model_dir else out_dir

    model_path = model_dir / "lsi_ml_model.cbm"
    meta_path = model_dir / "lsi_ml_metadata.joblib"
    if not model_path.exists() or not meta_path.exists():
        raise FileNotFoundError(
            f"Нет обученной модели в {model_dir}. "
            f"Сначала выполните полный retrain: python run_pipeline.py"
        )

    # 1) Features + stress components (теми же функциями, что в train — гарантия консистентности)
    features = build_daily_features(data_dir)
    df = compute_stress_components(features).copy().reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])

    # Per-indicator стресс-сигналы — отдельно, эти колонки нужны в X
    signals = build_indicator_signals(df)
    for col in signals.columns:
        df[col] = signals[col].values

    # 2) Loadmodel + metadata
    model = CatBoostRegressor()
    model.load_model(str(model_path))
    meta = joblib.load(meta_path)
    feature_columns = meta.get("feature_columns", ML_FEATURES)

    # 3) Predict + M4 multiplier
    seasonal = pd.to_numeric(df.get("m4_seasonal_factor", 1.0), errors="coerce")\
                 .fillna(1.0).clip(1.0, 1.4)
    X = df[feature_columns].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    base_pred = np.clip(model.predict(X), 0, 100)
    df["lsi_base"] = base_pred
    df["m4_multiplier"] = seasonal.values
    df["lsi_raw"] = np.clip(base_pred * seasonal.values, 0, 100)

    # 4) SHAP → contribution_M1..M5 + M4 как эффект мультипликатора
    pool = Pool(X, feature_names=feature_columns)
    shap = model.get_feature_importance(pool, type="ShapValues")
    shap_feats = shap[:, :-1]
    expected_value = shap[:, -1]
    df["lsi_baseline_expected"] = expected_value * seasonal.values
    for module in ("M1", "M2", "M3", "M5"):
        idxs = [i for i, f in enumerate(feature_columns)
                if FEATURE_TO_MODULE.get(f) == module]
        df[f"contribution_{module}"] = shap_feats[:, idxs].sum(axis=1) * seasonal.values
    df["contribution_M4"] = df["lsi_raw"] - df["lsi_base"]

    # 5) Kalman + hysteresis — копия из train_ml_lsi, шаг в шаг
    valid = df.get("full_model_valid", pd.Series(1, index=df.index))\
              .fillna(0).astype(int).eq(1)
    sort_idx = df["date"].argsort().to_numpy()
    smoothed_sorted = kalman_1d_smooth(df["lsi_raw"].to_numpy()[sort_idx])
    status_sorted = hysteresis_status(smoothed_sorted)
    smoothed = np.empty_like(smoothed_sorted)
    status_arr = np.empty(len(status_sorted), dtype=object)
    smoothed[sort_idx] = smoothed_sorted
    status_arr[sort_idx] = status_sorted
    df["lsi_smoothed"] = np.clip(smoothed, 0, 100)
    df["lsi"] = df["lsi_smoothed"].where(valid)
    df["status"] = status_arr
    df.loc[~valid, "status"] = "partial"
    df["status_raw"] = pd.cut(df["lsi_raw"], bins=[-np.inf, 40, 70, np.inf],
                              labels=["green", "yellow", "red"],
                              right=False).astype(str)

    # 6) Backtest на кризисных эпизодах (с уже обученной моделью)
    bt = _backtest_crisis(df)

    # 7) Сохраняем те же файлы, что и train_ml_lsi:
    #    - lsi_timeseries.csv (полный)
    #    - lsi_dashboard_extract.csv (слим)
    #    - backtest_crisis_episodes.csv
    # feature_importance / module_importance / .cbm / .joblib НЕ перезаписываем —
    # они от последнего retrain.
    safe_to_csv(df, out_dir / "lsi_timeseries.csv")
    safe_to_csv(df[[c for c in DASHBOARD_COLS if c in df.columns]],
                out_dir / "lsi_dashboard_extract.csv")
    safe_to_csv(bt, out_dir / "backtest_crisis_episodes.csv")

    print(f"Inference done. Rows: {len(df)}, valid: {int(valid.sum())}.")
    last_valid = df[valid].tail(1)
    if len(last_valid):
        r = last_valid.iloc[0]
        print(f"Latest LSI on {r['date'].date()}: "
              f"{r['lsi']:.1f} ({r['status']})")


if __name__ == "__main__":
    main()
