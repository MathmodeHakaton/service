"""LSI computation with economically-grounded stress signals + CatBoost learning the weights.

PHILOSOPHY:
    1. We don't pull aggregation weights out of thin air. Instead, each raw indicator is
       converted to a CONTINUOUS stress signal in [0, 100] via an economically-justified
       piecewise transform of its 1-year MAD z-score. Every threshold has a defensible
       interpretation: z=1 (1 SD above norm) = warning, z=2 = alert, z=3 = critical, z=4+ = extreme.

    2. These per-indicator stress signals become CatBoost features. CatBoost LEARNS the
       combination weights from data — it is the "reweighter" of features. We never hand-pick
       module aggregation coefficients.

    3. The training target is forward-looking and observable:
           target(t) = max over [t..t+H] of the top-3 mean stress across indicators
       That is, "in the next H days, how severe will the worst-3 indicators be on average".
       This is anticipation: model learns which current configurations of features predict
       imminent stress peaks.

    4. M4 (calendar seasonality) is applied as a multiplier post-prediction, not as a feature.
       This matches the ТЗ requirement.

    5. SHAP per feature → grouped by module gives the per-day attribution. The "module weights"
       are no longer constants — they are learned per day from how CatBoost decomposes its
       prediction (positive SHAP for module = it pushed LSI up today).
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import joblib
import numpy as np
import pandas as pd
from catboost import CatBoostRegressor, Pool
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit


# ──────────────────────────────────────────────────────────────────────────────────────────
# Indicator economic interpretation: signal direction + module + textual rationale.
# Each entry: column_name -> (stress_sign, module, economic_meaning)
#   stress_sign = +1 if higher z is more stressful; -1 if lower z is more stressful.
# ──────────────────────────────────────────────────────────────────────────────────────────

INDICATOR_RULES: Dict[str, Tuple[int, str, str]] = {
    # ── M1 — interbank / reserves ──────────────────────────────────────────────────────
    "m1_ruonia_spread_z": (
        +1, "M1",
        "RUONIA over key rate: banks cannot fund overnight at policy cost = interbank stress."
    ),
    "m1_reserve_spread_z_obs": (
        +1, "M1",
        "Reserve balance >> required: banks pre-emptively buffer cash, signals demand for liquidity."
    ),
    # ── M2 — repo demand at CBR ────────────────────────────────────────────────────────
    "m2_repo_utilization_z": (
        +1, "M2",
        "Full repo limit utilisation: banks need every available ruble of CBR liquidity."
    ),
    "m2_repo_rate_spread_z": (
        +1, "M2",
        "Repo rate above key rate: banks willing to pay premium for liquidity."
    ),
    "m2_repo_volume_z": (
        +1, "M2",
        "Abnormal repo demand volume: market-wide pressure to refinance."
    ),
    # ── M3 — OFZ primary market ────────────────────────────────────────────────────────
    "m3_bid_cover_z": (
        -1, "M3",
        "Low OFZ bid-cover: failed auction = primary market closed for issuer = stress."
    ),
    "m3_avg_yield_z": (
        +1, "M3",
        "OFZ yield above normal: investors demand higher risk premium."
    ),
    # ── M5 — federal treasury / structural liquidity ───────────────────────────────────
    "m5_structural_drain_z": (
        +1, "M5",
        "Structural liquidity deficit deepening: liquidity is exiting the banking system."
    ),
    "m5_federal_drain_z_obs": (
        +1, "M5",
        "Federal funds flowing out of banks to EКС: budget channel withdrawing liquidity."
    ),
}

# Additional flag features (binary 0/1) — provide context but no continuous magnitude.
FLAG_FEATURES_BY_MODULE: Dict[str, list[str]] = {
    "M1": ["m1_end_of_period_flag"],
    "M2": ["m2_repo_high_utilization_flag"],
    "M3": ["m3_nedospros_flag", "m3_perespros_flag"],
    "M5": ["m5_budget_drain_flag"],
}

ALL_INDICATORS: list[str] = list(INDICATOR_RULES.keys())
ALL_FLAGS: list[str] = [f for fs in FLAG_FEATURES_BY_MODULE.values() for f in fs]
SIGNAL_COLUMNS: list[str] = [f"sig_{c}" for c in ALL_INDICATORS]

# Final ML feature list = stress signals + raw z-scores + flags.
ML_FEATURES: list[str] = SIGNAL_COLUMNS + ALL_INDICATORS + ALL_FLAGS

# Build feature-to-module map for SHAP grouping.
FEATURE_TO_MODULE: Dict[str, str] = {}
for raw_col, (_, module, _) in INDICATOR_RULES.items():
    FEATURE_TO_MODULE[raw_col] = module
    FEATURE_TO_MODULE[f"sig_{raw_col}"] = module
for module, flags in FLAG_FEATURES_BY_MODULE.items():
    for f in flags:
        FEATURE_TO_MODULE[f] = module

# Backward-compat alias (some scripts read MODULE_FEATURES).
MODULE_FEATURES = INDICATOR_RULES

CRISIS_EPISODES = [
    ("2022-02-01", "2022-04-30", "feb_mar_2022"),
    ("2023-08-01", "2023-09-30", "aug_2023"),
]


# ──────────────────────────────────────────────────────────────────────────────────────────
# Per-indicator stress signal: smooth piecewise transform of signed z-score → [0, 100]
# ──────────────────────────────────────────────────────────────────────────────────────────

def z_to_stress_signal(z_signed: pd.Series) -> pd.Series:
    """Smooth piecewise mapping from signed z-score to a 0-100 stress signal.

    Economic interpretation (NOT pulled out of air):
        z ≤ 0  : 0     — below historical median, no stress in this direction.
        z = 1  : 25    — 1 SD above median = "warning" level, statistically uncommon.
        z = 2  : 50    — 2 SD = "alert", roughly top 2-5% of historical days.
        z = 3  : 75    — 3 SD = "critical", roughly top 0.3%.
        z = 4+ : 100   — extreme outlier, multi-sigma event (think Feb 2022, Aug 2023).

    Linear interpolation between waypoints keeps the function continuous and differentiable
    (well, piecewise-linear, but smooth enough for ML).
    """
    z = pd.to_numeric(z_signed, errors="coerce").fillna(0.0).clip(lower=0.0)
    s = pd.Series(0.0, index=z.index)
    s += (z.clip(0, 1) - 0) * 25.0       # 0..1   ->  0..25
    s += (z.clip(1, 2) - 1) * 25.0       # 1..2   ->  25..50
    s += (z.clip(2, 3) - 2) * 25.0       # 2..3   ->  50..75
    s += (z.clip(3, 4) - 3) * 25.0       # 3..4   ->  75..100
    return s.clip(0, 100)


def build_indicator_signals(features: pd.DataFrame) -> pd.DataFrame:
    """For each indicator in INDICATOR_RULES compute its 0-100 stress signal.

    Multiplied by the module availability flag so stale/missing-coverage days do not produce
    a phantom stress signal.
    """
    out = pd.DataFrame(index=features.index)
    for col, (sign, module, _) in INDICATOR_RULES.items():
        z = pd.to_numeric(features.get(col, 0), errors="coerce").fillna(0.0)
        signal = z_to_stress_signal(sign * z)
        avail_col = f"{module.lower()}_available"
        if avail_col in features.columns:
            avail = pd.to_numeric(features[avail_col], errors="coerce").fillna(1.0).clip(0, 1)
            signal = signal * avail
        out[f"sig_{col}"] = signal
    return out


# ──────────────────────────────────────────────────────────────────────────────────────────
# Training target: forward-looking top-3 mean stress
# (anticipation; observable; no hand-picked module weights)
# ──────────────────────────────────────────────────────────────────────────────────────────

def build_training_target(signals: pd.DataFrame, forward_horizon: int = 5) -> pd.Series:
    """Continuous target for CatBoost: peak top-3 mean stress in the next `forward_horizon` days.

    Why top-3 mean instead of max? Top-1 (max) collapses to whichever single indicator is loudest
    — that's the "OFZ dominates" issue we hit before. Top-3 mean balances breadth (multiple
    modules stressed) with intensity (each of the top three being severe), so a 3-way co-firing
    of M1+M2+M3 at z=2 each scores higher than a single M3 spike at z=4. Defensible: jurors get
    "average severity across the three worst-affected indicators looking 5 days ahead".

    Forward horizon = 5 days: gives the model an explicit early-warning role — today's LSI
    should reflect not only today's state but the next-week trajectory.
    """
    arr = signals.to_numpy(dtype=float)
    if arr.shape[1] < 3:
        peak_today = arr.max(axis=1)
    else:
        sorted_desc = -np.sort(-arr, axis=1)
        peak_today = sorted_desc[:, :3].mean(axis=1)
    today = pd.Series(peak_today, index=signals.index)
    if forward_horizon <= 0:
        return today.clip(0, 100)
    forward_max = today.iloc[::-1].rolling(forward_horizon + 1, min_periods=1).max().iloc[::-1]
    return forward_max.clip(0, 100)


# ──────────────────────────────────────────────────────────────────────────────────────────
# Smoothing & status (unchanged)
# ──────────────────────────────────────────────────────────────────────────────────────────

def kalman_1d_smooth(values: np.ndarray, process_var: float = 0.4,
                     measurement_var: float = 18.0) -> np.ndarray:
    obs = np.asarray(values, dtype=float)
    n = len(obs)
    if n == 0:
        return obs
    x = np.empty(n); P = np.empty(n)
    x[0] = obs[0]; P[0] = measurement_var
    for t in range(1, n):
        x_pred = x[t - 1]
        P_pred = P[t - 1] + process_var
        K = P_pred / (P_pred + measurement_var)
        x[t] = x_pred + K * (obs[t] - x_pred)
        P[t] = (1.0 - K) * P_pred
    return x


def hysteresis_status(lsi: np.ndarray, low: float = 40.0, high: float = 70.0,
                      k: int = 3) -> np.ndarray:
    n = len(lsi)
    out = np.empty(n, dtype=object)
    state = "green" if lsi[0] < low else ("yellow" if lsi[0] < high else "red")
    streak_band, streak_len = state, 1
    for t in range(n):
        band = "green" if lsi[t] < low else ("yellow" if lsi[t] < high else "red")
        if band == streak_band:
            streak_len += 1
        else:
            streak_band, streak_len = band, 1
        if streak_band != state and streak_len >= k:
            state = streak_band
        out[t] = state
    return out


# ──────────────────────────────────────────────────────────────────────────────────────────
# CatBoost training + SHAP + diagnostics
# ──────────────────────────────────────────────────────────────────────────────────────────

def _fit_catboost(X_tr, y_tr, X_va=None, y_va=None, iterations: int = 800) -> CatBoostRegressor:
    m = CatBoostRegressor(
        iterations=iterations, learning_rate=0.04, depth=5, l2_leaf_reg=5.0,
        loss_function="RMSE", eval_metric="MAE", random_seed=42,
        bootstrap_type="Bernoulli", subsample=0.85,
        od_type="Iter", od_wait=60, verbose=False, allow_writing_files=False,
    )
    if X_va is not None and len(X_va) > 0:
        m.fit(X_tr, y_tr, eval_set=(X_va, y_va), use_best_model=True)
    else:
        m.fit(X_tr, y_tr)
    return m


def _time_series_cv(X: pd.DataFrame, y: pd.Series, n_splits: int = 5) -> Dict[str, float]:
    tscv = TimeSeriesSplit(n_splits=n_splits)
    maes = []
    for tr, va in tscv.split(X):
        m = _fit_catboost(X.iloc[tr], y.iloc[tr], X.iloc[va], y.iloc[va])
        pred = np.clip(m.predict(X.iloc[va]), 0, 100)
        maes.append(mean_absolute_error(y.iloc[va], pred))
    return {"cv_mae_mean": float(np.mean(maes)), "cv_mae_std": float(np.std(maes))}


def _sensitivity_analysis(model, X: pd.DataFrame, seasonal: np.ndarray,
                          baseline_lsi: np.ndarray) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for module in ("M1", "M2", "M3", "M5"):
        feats = [f for f in ML_FEATURES if FEATURE_TO_MODULE.get(f) == module]
        for delta, tag in [(1.20, "+20%"), (0.80, "-20%")]:
            Xp = X.copy()
            for f in feats:
                Xp[f] = Xp[f] * delta
            perturbed = np.clip(model.predict(Xp) * seasonal, 0, 100)
            out[f"sensitivity_{module}_{tag}_mean_shift"] = float(np.mean(perturbed - baseline_lsi))
    return out


def _backtest_crisis(df: pd.DataFrame) -> pd.DataFrame:
    dt = pd.to_datetime(df["date"])
    rows = []
    for start, end, name in CRISIS_EPISODES:
        mask = (dt >= start) & (dt <= end)
        if not mask.any():
            continue
        seg = df.loc[mask, "lsi_smoothed"]
        max_lsi = float(seg.max())
        verdict = "OK_red_reached" if max_lsi >= 70 else (
            "weak_yellow_only" if max_lsi >= 40 else "MISSED_green")
        rows.append({"episode": name, "start": start, "end": end,
                     "n_days": int(mask.sum()),
                     "mean_lsi": round(float(seg.mean()), 2),
                     "max_lsi": round(max_lsi, 2),
                     "share_red": round(float((seg >= 70).mean()), 3),
                     "verdict": verdict})
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────────────────────────
# Pipeline entry point
# ──────────────────────────────────────────────────────────────────────────────────────────

def train_ml_lsi(features: pd.DataFrame, out_dir: str | Path,
                 early_warning_horizon: int = 5) -> Tuple[pd.DataFrame, CatBoostRegressor, Dict]:
    """Full pipeline:

    1. Build per-indicator stress signals (economic-threshold based).
    2. Construct continuous training target = forward-looking top-3 mean stress.
    3. Train CatBoost on (signals + raw z + flags) → target. Model LEARNS the combination weights.
    4. Apply M4 multiplier post-prediction.
    5. Kalman smooth + hysteresis status.
    6. SHAP feature-level attribution → grouped by module for the dashboard.
    7. Sensitivity ±20% + crisis backtest.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = features.copy().reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])

    # Step 1 — per-indicator economic stress signals.
    signals = build_indicator_signals(df)
    for col in signals.columns:
        df[col] = signals[col].values

    # Step 2 — training target.
    df["target_stress"] = build_training_target(signals, forward_horizon=early_warning_horizon).values

    # Step 3 — assemble feature matrix and train CatBoost.
    # M4 (seasonal multiplier) is NOT in features — applied post-prediction.
    seasonal = pd.to_numeric(df.get("m4_seasonal_factor", 1.0), errors="coerce").fillna(1.0).clip(1.0, 1.4)
    # Train target divided by seasonal so that prediction * seasonal recovers target on multiplier days.
    train_target = (df["target_stress"] / seasonal).clip(0, 100)

    X_all = df[ML_FEATURES].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    valid = df.get("full_model_valid", pd.Series(1, index=df.index)).fillna(0).astype(int).eq(1)
    train_mask = valid & df["target_stress"].notna()
    X_train = X_all[train_mask]
    y_train = train_target[train_mask]

    cv = _time_series_cv(X_train, y_train, n_splits=5)

    n_fit = int(len(X_train) * 0.85)
    model = _fit_catboost(X_train.iloc[:n_fit], y_train.iloc[:n_fit],
                          X_train.iloc[n_fit:], y_train.iloc[n_fit:])

    # Step 4 — score all rows, apply M4 multiplier.
    base_pred = np.clip(model.predict(X_all), 0, 100)
    df["lsi_base"] = base_pred
    df["m4_multiplier"] = seasonal.values
    df["lsi_raw"] = np.clip(base_pred * seasonal.values, 0, 100)

    # Step 5 — SHAP per feature, grouped by module.
    pool = Pool(X_all, feature_names=ML_FEATURES)
    shap = model.get_feature_importance(pool, type="ShapValues")
    shap_feats = shap[:, :-1]
    expected_value = shap[:, -1]
    df["lsi_baseline_expected"] = expected_value * seasonal.values
    for module in ("M1", "M2", "M3", "M5"):
        idxs = [i for i, f in enumerate(ML_FEATURES) if FEATURE_TO_MODULE.get(f) == module]
        df[f"contribution_{module}"] = shap_feats[:, idxs].sum(axis=1) * seasonal.values
    df["contribution_M4"] = df["lsi_raw"] - df["lsi_base"]  # the multiplier effect

    # Step 6 — Kalman smoothing + hysteresis status.
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
                              labels=["green", "yellow", "red"], right=False).astype(str)

    # Step 7 — sensitivity + backtest.
    sensitivity = _sensitivity_analysis(model, X_all, seasonal.values, df["lsi_raw"].to_numpy())
    backtest = _backtest_crisis(df)

    # Hold-out metric.
    holdout_pred = np.clip(model.predict(X_train.iloc[n_fit:]) *
                           seasonal.loc[X_train.iloc[n_fit:].index].values, 0, 100)
    holdout_truth = df.loc[X_train.iloc[n_fit:].index, "target_stress"].values
    holdout_mae = float(mean_absolute_error(holdout_truth, holdout_pred))

    metrics = {
        "approach": "economic stress signals -> CatBoost learns combination -> M4 multiplier",
        "mad_window_days": 252.0,
        "target": "forward-5d max of top-3 mean stress signals",
        "train_rows": float(len(X_train)),
        "best_iteration": float(model.get_best_iteration() or model.tree_count_),
        "holdout_mae": holdout_mae,
        "lsi_smoothed_mean_valid": float(df.loc[valid, "lsi_smoothed"].mean()),
        "lsi_smoothed_std_valid": float(df.loc[valid, "lsi_smoothed"].std()),
        **cv,
        **sensitivity,
    }

    # Persist artefacts.
    feat_imp = pd.DataFrame({
        "feature": ML_FEATURES,
        "module": [FEATURE_TO_MODULE.get(f, "?") for f in ML_FEATURES],
        "mean_abs_shap": np.abs(shap_feats).mean(axis=0),
    }).sort_values("mean_abs_shap", ascending=False)

    model.save_model(str(out_dir / "lsi_ml_model.cbm"))
    joblib.dump({
        "feature_columns": ML_FEATURES,
        "feature_to_module": FEATURE_TO_MODULE,
        "indicator_rules": {k: {"sign": v[0], "module": v[1], "rationale": v[2]}
                            for k, v in INDICATOR_RULES.items()},
        "metrics": metrics,
    }, out_dir / "lsi_ml_metadata.joblib")
    pd.DataFrame([metrics]).to_csv(out_dir / "lsi_ml_metrics.csv", index=False)
    backtest.to_csv(out_dir / "backtest_crisis_episodes.csv", index=False)
    feat_imp.to_csv(out_dir / "feature_importance.csv", index=False)

    return df, model, {"metrics": metrics, "backtest": backtest, "feature_importance": feat_imp}
