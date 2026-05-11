from __future__ import annotations

import numpy as np
import pandas as pd

MARKET_MODULES = ["M1", "M2", "M3", "M5"]

COMPONENT_GROUPS = {
    "M1": [
        "m1_reserve_spread_stress",
        "m1_ruonia_spread_stress",
        "m1_end_period_context",
        "m1_calm_score",
        "m1_net_signal",
    ],
    "M2": [
        "m2_repo_volume_stress",
        "m2_repo_utilization_stress",
        "m2_repo_rate_spread_stress",
        "m2_high_utilization_flag",
        "m2_calm_score",
        "m2_net_signal",
    ],
    "M3": [
        "m3_ofz_low_cover_stress",
        "m3_nedospros_static_stress",
        "m3_nedospros_flag",
        "m3_yield_stress",
        "m3_perespros_anti_stress",
        "m3_calm_score",
        "m3_net_signal",
    ],
    "M4": [
        "m4_tax_week_flag",
        "m4_end_of_month_flag",
        "m4_end_of_quarter_flag",
        "m4_seasonal_factor",
    ],
    "M5": [
        "m5_structural_drain_stress",
        "m5_federal_drain_stress",
        "m5_budget_drain_flag",
        "m5_calm_score",
        "m5_net_signal",
    ],
    "COHERENCE": [
        "active_market_modules_count",
        "active_components_count",
        "max_component_stress",
        "top2_component_mean",
        "top3_component_mean",
        "stress_breadth_score",
        "stress_persistence_3d",
        "calm_modules_count",
        "mean_calm_score",
        "net_market_signal",
    ],
    "INTERACTION": [
        "m2_m5_interaction",
        "m2_m3_interaction",
        "m3_m5_interaction",
        "m1_m2_interaction",
        "m1_m5_interaction",
        "m2_m3_m5_interaction",
    ],
}


def _num(s, index=None) -> pd.Series:
    if isinstance(s, pd.Series):
        return pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    if index is None:
        return pd.Series(float(s))
    return pd.Series(float(s), index=index)


def pos_z(s, cap: float = 8.0) -> pd.Series:
    """Stress component for indicators where upward deviation means stress."""
    return _num(s).clip(lower=0.0, upper=cap)


def neg_z(s, cap: float = 8.0) -> pd.Series:
    """Stress component for indicators where downward deviation means stress."""
    return (-_num(s)).clip(lower=0.0, upper=cap)


def bounded_flag(s) -> pd.Series:
    return _num(s).clip(lower=0.0, upper=1.0)


def calm_band(s, half_width: float = 0.7) -> pd.Series:
    """Calm evidence when |z| is well within +/- half_width. 1.0 at z=0, 0.0 at |z|>=half_width."""
    z = _num(s).abs()
    return ((half_width - z) / half_width).clip(lower=0.0, upper=1.0)


def calm_below(s, threshold: float = -0.3, scale: float = 1.5) -> pd.Series:
    """Calm evidence when indicator is well below threshold (e.g. low repo utilisation, liquidity inflow)."""
    z = _num(s)
    return ((threshold - z) / scale).clip(lower=0.0, upper=2.0)


def calm_above(s, threshold: float = 0.3, scale: float = 1.5) -> pd.Series:
    """Calm evidence when indicator is well above threshold (e.g. healthy OFZ bid cover)."""
    z = _num(s)
    return ((z - threshold) / scale).clip(lower=0.0, upper=2.0)


def row_top_mean(df: pd.DataFrame, cols: list[str], k: int) -> pd.Series:
    if not cols:
        return pd.Series(0.0, index=df.index)
    values = df[cols].fillna(0.0).to_numpy(dtype=float)
    if values.shape[1] == 0:
        return pd.Series(0.0, index=df.index)
    k = min(k, values.shape[1])
    part = np.partition(values, -k, axis=1)[:, -k:]
    return pd.Series(part.mean(axis=1), index=df.index)


def compute_stress_components(features: pd.DataFrame) -> pd.DataFrame:
    """Convert daily features into atomic stress components.

    There are intentionally no hand-made module scores here. Each raw indicator becomes
    an atomic stress component after rolling MAD z-score and stress direction. CatBoost
    later learns how to combine these components into LSI.
    """
    df = features.copy()

    # M1: reserve spread and RUONIA/keyrate spread are stressful when unusually high.
    df["m1_reserve_spread_stress"] = pos_z(df.get("m1_reserve_spread_z_obs", 0))
    df["m1_ruonia_spread_stress"] = pos_z(df.get("m1_ruonia_spread_z", 0))
    # This is context inside M1, not a weighted score: it allows the model to learn that
    # the same deviation near averaging-period end can mean more than mid-period.
    df["m1_end_period_context"] = bounded_flag(df.get("m1_end_of_period_flag", 0))

    # M2: repo stress is high volume/utilization/rate spread.
    df["m2_repo_volume_stress"] = pos_z(df.get("m2_repo_volume_z", 0))
    df["m2_repo_utilization_stress"] = pos_z(df.get("m2_repo_utilization_z", 0))
    df["m2_repo_rate_spread_stress"] = pos_z(df.get("m2_repo_rate_spread_z", 0))
    df["m2_high_utilization_flag"] = bounded_flag(df.get("m2_repo_high_utilization_flag", 0))

    # M3: OFZ stress is LOW cover ratio. Perespros is an anti-stress context feature.
    df["m3_ofz_low_cover_stress"] = neg_z(df.get("m3_bid_cover_z", 0))
    df["m3_nedospros_static_stress"] = _num(df.get("m3_nedospros_static_score", 0)).clip(0.0, 6.0)
    df["m3_nedospros_flag"] = bounded_flag(df.get("m3_nedospros_flag", 0))
    df["m3_yield_stress"] = pos_z(df.get("m3_avg_yield_z", 0))
    # Positive value here means 'anti-stress evidence'. CatBoost can learn that it lowers LSI.
    df["m3_perespros_anti_stress"] = bounded_flag(df.get("m3_perespros_flag", 0))

    # M4: calendar context, not independent stress.
    df["m4_tax_week_flag"] = bounded_flag(df.get("m4_tax_week_flag", 0))
    df["m4_end_of_month_flag"] = bounded_flag(df.get("m4_end_of_month_flag", 0))
    df["m4_end_of_quarter_flag"] = bounded_flag(df.get("m4_end_of_quarter_flag", 0))
    df["m4_seasonal_factor"] = _num(df.get("m4_seasonal_factor", 1.0), index=df.index).clip(1.0, 1.4)

    # M5: liquidity drain is stress.
    df["m5_structural_drain_stress"] = pos_z(df.get("m5_structural_drain_z", 0))
    df["m5_federal_drain_stress"] = pos_z(df.get("m5_federal_drain_z_obs", 0))
    df["m5_budget_drain_flag"] = bounded_flag(df.get("m5_budget_drain_flag", 0))

    # --- Calm (signed) evidence per module ---------------------------------------------------
    # Each m{i}_calm_score is non-negative; it measures how strongly the module signals calm.
    # m{i}_net_signal = stress_max - calm_score is the signed module reading: positive = stress,
    # negative = active calm evidence. This is the "signed component" view the bank reviewer
    # reads as a thermometer with sign.

    # M1: calm when RUONIA hugs keyrate and reserve spread is within band.
    df["m1_calm_ruonia"] = calm_band(df.get("m1_ruonia_spread_z", 0))
    df["m1_calm_reserve"] = calm_band(df.get("m1_reserve_spread_z_obs", 0))
    df["m1_calm_score"] = df[["m1_calm_ruonia", "m1_calm_reserve"]].mean(axis=1)

    # M2: calm when repo utilisation is low, volume below median, rate spread below median.
    df["m2_calm_util"] = calm_below(df.get("m2_repo_utilization_z", 0))
    df["m2_calm_volume"] = calm_below(df.get("m2_repo_volume_z", 0))
    df["m2_calm_rate"] = calm_below(df.get("m2_repo_rate_spread_z", 0))
    df["m2_calm_score"] = df[["m2_calm_util", "m2_calm_volume", "m2_calm_rate"]].mean(axis=1)
    # High-utilisation flag cancels any calm evidence in M2.
    df["m2_calm_score"] = df["m2_calm_score"] * (1.0 - df["m2_high_utilization_flag"])

    # M3: calm when OFZ demand is healthy (bid cover above median) and yields near median.
    df["m3_calm_cover"] = calm_above(df.get("m3_bid_cover_z", 0))
    df["m3_calm_yield"] = calm_band(df.get("m3_avg_yield_z", 0))
    df["m3_calm_score"] = df[["m3_calm_cover", "m3_calm_yield"]].mean(axis=1)
    # Nedospros (failed auction) cancels calm evidence.
    df["m3_calm_score"] = df["m3_calm_score"] * (1.0 - df["m3_nedospros_flag"])

    # M5: calm when liquidity is flowing in (negative drain z), not out.
    df["m5_calm_struct"] = calm_below(df.get("m5_structural_drain_z", 0))
    df["m5_calm_federal"] = calm_below(df.get("m5_federal_drain_z_obs", 0))
    df["m5_calm_score"] = df[["m5_calm_struct", "m5_calm_federal"]].mean(axis=1)
    df["m5_calm_score"] = df["m5_calm_score"] * (1.0 - df["m5_budget_drain_flag"])

    # Availability: if module is absent/stale, zero out its components. This avoids a stale OFZ
    # observation or missing M5 value producing fake model evidence.
    for module in MARKET_MODULES:
        prefix = module.lower()
        available = bounded_flag(df.get(f"{prefix}_available", 1))
        for c in COMPONENT_GROUPS[module]:
            if c in df.columns:
                df[c] = df[c] * available

    # Module max is not a manual module score. It is a neutral summary used for state labels,
    # interactions and model features: 'how strong is the strongest current signal in the module?'
    # Exclude calm/net columns from the max so calm evidence does not inflate the stress max.
    for module in MARKET_MODULES:
        stress_cols = [c for c in COMPONENT_GROUPS[module]
                       if not c.endswith("_calm_score") and not c.endswith("_net_signal")]
        low = module.lower()
        df[f"{low}_max_component"] = df[stress_cols].max(axis=1).fillna(0.0)
        df[f"{low}_top2_mean"] = row_top_mean(df, stress_cols, 2)
        # Signed module reading: positive = net stress, negative = net calm.
        df[f"{low}_net_signal"] = df[f"{low}_max_component"] - df[f"{low}_calm_score"]
        # Active threshold is now on the *signed* signal: a module counts as active stress only
        # if its stress evidence is not offset by simultaneous calm evidence. This is the
        # cascade-fix that makes "active" robust to the post-2022 baseline drift in raw z-scores.
        # Threshold 2.5 (was 2.0) compensates for the smaller winsorized-MAD denominator
        # which inflates z-scores by ~15-25% on average. With the higher threshold, calm periods
        # stay non-active and only genuine multi-z-unit deviations count as module activity.
        df[f"{low}_active"] = (df[f"{low}_net_signal"] >= 2.5).astype(int)

    # Flags can activate a module even when z history is short.
    df["m2_active"] = ((df["m2_active"] == 1) | (df["m2_high_utilization_flag"] == 1)).astype(int)
    df["m3_active"] = ((df["m3_active"] == 1) | (df["m3_nedospros_flag"] == 1)).astype(int)
    df["m5_active"] = ((df["m5_active"] == 1) | (df["m5_budget_drain_flag"] == 1)).astype(int)

    # Cross-module coherence features. These are not weights; they describe market regime.
    active_cols = ["m1_active", "m2_active", "m3_active", "m5_active"]
    df["active_market_modules_count"] = df[active_cols].sum(axis=1)

    # Calm-side aggregates: how many modules are demonstrably calm, and how strong on average.
    calm_cols = [f"{m.lower()}_calm_score" for m in MARKET_MODULES]
    net_cols = [f"{m.lower()}_net_signal" for m in MARKET_MODULES]
    df["calm_modules_count"] = (df[calm_cols] >= 0.5).sum(axis=1)
    df["mean_calm_score"] = df[calm_cols].mean(axis=1)
    # net_market_signal is the sum of per-module signed signals — central thermometer of the market.
    df["net_market_signal"] = df[net_cols].sum(axis=1)

    atomic_stress_cols = []
    for module in MARKET_MODULES:
        for c in COMPONENT_GROUPS[module]:
            if not c.endswith("anti_stress") and not c.endswith("context"):
                atomic_stress_cols.append(c)

    df["active_components_count"] = (df[atomic_stress_cols] >= 2.0).sum(axis=1)
    df["max_component_stress"] = df[atomic_stress_cols].max(axis=1).fillna(0.0)
    df["top2_component_mean"] = row_top_mean(df, atomic_stress_cols, 2)
    df["top3_component_mean"] = row_top_mean(df, atomic_stress_cols, 3)
    df["stress_breadth_score"] = (
        0.6 * df["active_market_modules_count"].clip(0, 4)
        + 0.4 * df["active_components_count"].clip(0, 8) / 2.0
    )

    # Persistence helps early-warning: single-day spikes get less credibility than a signal
    # that survives for several days.
    severe_today = (df["active_market_modules_count"] >= 2).astype(float)
    df["stress_persistence_3d"] = severe_today.rolling(3, min_periods=1).mean().fillna(0.0)

    # Interaction features, scaled in z-units. CatBoost can learn interactions itself, but explicit
    # interactions make the design easier to explain to business/experts.
    df["m2_m5_interaction"] = np.sqrt(df["m2_max_component"].clip(0) * df["m5_max_component"].clip(0))
    df["m2_m3_interaction"] = np.sqrt(df["m2_max_component"].clip(0) * df["m3_max_component"].clip(0))
    df["m3_m5_interaction"] = np.sqrt(df["m3_max_component"].clip(0) * df["m5_max_component"].clip(0))
    df["m1_m2_interaction"] = np.sqrt(df["m1_max_component"].clip(0) * df["m2_max_component"].clip(0))
    df["m1_m5_interaction"] = np.sqrt(df["m1_max_component"].clip(0) * df["m5_max_component"].clip(0))
    df["m2_m3_m5_interaction"] = np.cbrt(
        df["m2_max_component"].clip(0) * df["m3_max_component"].clip(0) * df["m5_max_component"].clip(0)
    )

    return df


def model_feature_columns() -> list[str]:
    cols: list[str] = []
    for group in ["M1", "M2", "M3", "M4", "M5", "COHERENCE", "INTERACTION"]:
        cols.extend(COMPONENT_GROUPS[group])
    cols.extend([
        "m1_max_component", "m1_top2_mean", "m1_active",
        "m2_max_component", "m2_top2_mean", "m2_active",
        "m3_max_component", "m3_top2_mean", "m3_active",
        "m5_max_component", "m5_top2_mean", "m5_active",
        "coverage_score",
    ])
    # preserve order, remove duplicates
    return list(dict.fromkeys(cols))


def feature_to_module() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for module, cols in COMPONENT_GROUPS.items():
        for c in cols:
            mapping[c] = module
    for module in MARKET_MODULES:
        low = module.lower()
        mapping[f"{low}_max_component"] = module
        mapping[f"{low}_top2_mean"] = module
        mapping[f"{low}_active"] = module
    mapping["coverage_score"] = "COVERAGE"
    return mapping
