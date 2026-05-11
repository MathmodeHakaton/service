from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

CRISIS_WINDOWS = [
    ("2014-12-01", "2015-01-31", "dec_2014"),
    ("2022-02-01", "2022-04-30", "feb_mar_2022"),
    ("2023-08-01", "2023-09-30", "aug_2023"),
]

# We are allowed to spread lower-frequency observations to days, but not through a long data break.
# These caps define when a last observation becomes stale and must NOT be carried further.
MAX_CARRY_DAYS = {
    "m1_reserve": 45,       # monthly / averaging-period data
    "m1_ruonia": 7,         # daily business-day rate, weekends/holidays only
    "keyrate": 370,         # policy rate stays valid until a new decision
    "m2_repo": 21,          # weekly/event repo, do not carry through long breaks
    "m3_ofz": 35,           # OFZ usually weekly/biweekly; do not fill multi-month/year gaps
    "m5_bliquidity": 14,    # operational structural liquidity table
    "m5_federal": 45,       # monthly SORS federal funds on bank accounts
}


def _read_csv(data_dir: Path, name: str) -> pd.DataFrame:
    p = data_dir / name
    if not p.exists():
        raise FileNotFoundError(f"Missing input file: {p}")
    df = pd.read_csv(p)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")
    return df


def robust_mad_z(s: pd.Series, window: int = 252, min_periods: int = 60,
                 winsor_q: float = 0.01) -> pd.Series:
    """Past-only rolling robust z-score with winsorized MAD.

    Window: 252 trading days = 1 year. Min periods: 60 (3 months) for warmup.
    Shorter window means the baseline adapts faster to regime changes — important for the
    post-2022 Russian money market where the "normal" liquidity profile has shifted. With a
    3-year window, the MAD denominator was inflated by 2022 shock for years after, compressing
    z-scores and making the LSI look flat across 2022-2026. The 1-year window fixes that.

    Uses median and MAD shifted by one day, so today's score does not see today's value inside
    the baseline (honest backtest). Winsorization (1% tails) prevents a single extreme day
    from dominating the MAD denominator within the window.
    """
    s = pd.to_numeric(s, errors="coerce")
    med = s.rolling(window=window, min_periods=min_periods).median().shift(1)

    def _winsorized_mad(x):
        x = np.asarray(x, dtype=float)
        x = x[np.isfinite(x)]
        if len(x) < 20:
            return np.nan if len(x) == 0 else np.median(np.abs(x - np.median(x)))
        lo, hi = np.quantile(x, [winsor_q, 1.0 - winsor_q])
        trimmed = x[(x >= lo) & (x <= hi)]
        if len(trimmed) == 0:
            trimmed = x
        m = np.median(trimmed)
        return np.median(np.abs(trimmed - m))

    mad = s.rolling(window=window, min_periods=min_periods).apply(_winsorized_mad, raw=True).shift(1)
    denom = 1.4826 * mad.replace(0, np.nan)
    z = (s - med) / denom
    return z.replace([np.inf, -np.inf], np.nan).clip(-10, 10)


def asof_spread(calendar: pd.DataFrame, df: pd.DataFrame, prefix: str, max_carry_days: int | None = None) -> pd.DataFrame:
    """Backward as-of merge with a maximum carry period.

    This implements the agreed daily spreading: monthly/weekly/event values are copied to
    daily rows until the next observation, but only while the observation remains within
    max_carry_days. A multi-year OFZ hole therefore stays a hole, not a fake flat signal.
    """
    cols = [c for c in df.columns if c != "date"]
    tmp = df.copy().sort_values("date")
    tmp[f"{prefix}_obs_date"] = tmp["date"]
    out = pd.merge_asof(calendar.sort_values("date"), tmp, on="date", direction="backward")
    out[f"{prefix}_days_since_update"] = (out["date"] - out[f"{prefix}_obs_date"]).dt.days
    max_days = MAX_CARRY_DAYS.get(prefix, max_carry_days)
    if max_days is not None:
        stale = out[f"{prefix}_days_since_update"].isna() | (out[f"{prefix}_days_since_update"] > max_days)
        for c in cols:
            out.loc[stale, c] = np.nan
    return out[["date", f"{prefix}_obs_date", f"{prefix}_days_since_update"] + cols]


def _normalize_ofz(data_dir: Path) -> pd.DataFrame:
    """Read either the new full OFZ file or the old 2026-only file and normalize columns."""
    full = data_dir / "m3_ofz_full.csv"
    old = data_dir / "m3_ofz_auctions.csv"
    if full.exists():
        raw = pd.read_csv(full)
        # New Russian-column file.
        raw["date"] = pd.to_datetime(raw["Дата"], errors="coerce")
        out = pd.DataFrame({
            "date": raw["date"],
            "issue": raw.get("Код выпуска"),
            "security_type": raw.get("Тип бумаги"),
            "offer_volume": pd.to_numeric(raw.get("Объем предложения, млн руб."), errors="coerce"),
            "demand_volume": pd.to_numeric(raw.get("Совокупный объем спроса по номиналу, млн руб."), errors="coerce"),
            "placement_volume": pd.to_numeric(raw.get("Объем размещения по номиналу, млн руб."), errors="coerce"),
            "avg_yield": pd.to_numeric(raw.get("Доходность средневзвешенная, % годовых"), errors="coerce"),
            "yield_cutoff": pd.to_numeric(raw.get("Доходность по цене отсечения, % годовых"), errors="coerce"),
        })
        out["source_file"] = "m3_ofz_full.csv"
        return out.dropna(subset=["date"]).sort_values("date")
    if old.exists():
        raw = pd.read_csv(old)
        raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
        out = pd.DataFrame({
            "date": raw["date"],
            "issue": raw.get("issue"),
            "security_type": raw.get("Тип бумаги"),
            "offer_volume": pd.to_numeric(raw.get("offer_volume"), errors="coerce"),
            "demand_volume": pd.to_numeric(raw.get("demand_volume"), errors="coerce"),
            "placement_volume": pd.to_numeric(raw.get("placement_volume"), errors="coerce"),
            "avg_yield": pd.to_numeric(raw.get("avg_yield"), errors="coerce"),
            "yield_cutoff": pd.to_numeric(raw.get("Доходность по цене отсечения"), errors="coerce"),
        })
        out["source_file"] = "m3_ofz_auctions.csv"
        return out.dropna(subset=["date"]).sort_values("date")
    raise FileNotFoundError("Missing OFZ file: expected m3_ofz_full.csv or m3_ofz_auctions.csv")


def build_daily_features(data_dir: str | Path, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    data_dir = Path(data_dir)
    m1_reserves = _read_csv(data_dir, "m1_reserves.csv")
    m1_ruonia = _read_csv(data_dir, "m1_ruonia.csv")
    keyrate = _read_csv(data_dir, "m2_keyrate.csv")
    repo_auc = _read_csv(data_dir, "m2_repo_auctions.csv")
    repo_params = _read_csv(data_dir, "m2_repo_params.csv")
    tax = _read_csv(data_dir, "m4_tax_calendar.csv")
    bliq = _read_csv(data_dir, "m5_bliquidity.csv")
    fed = _read_csv(data_dir, "m5_sors_federal_funds.csv")
    ofz = _normalize_ofz(data_dir)

    if start is None:
        # Full five-module model is impossible before OFZ starts; keep 2014 in crisis metadata,
        # but build final AE calendar from the maximum of M3 start and requested start.
        start = max(pd.Timestamp("2014-01-01"), ofz["date"].min()).strftime("%Y-%m-%d")
    if end is None:
        candidates = [m1_ruonia["date"].max(), keyrate["date"].max(), bliq["date"].max(), repo_auc["date"].max(), ofz["date"].max()]
        end = max([d for d in candidates if pd.notna(d)]).strftime("%Y-%m-%d")

    calendar = pd.DataFrame({"date": pd.date_range(start=start, end=end, freq="D")})
    df = calendar.copy()

    # M1 reserves + RUONIA
    m1 = m1_reserves.copy()
    m1["m1_reserve_spread_bln"] = m1["actual_avg_bln"] - m1["required_avg_bln"]
    m1["m1_reserve_spread_z_obs"] = robust_mad_z(m1["m1_reserve_spread_bln"], window=36, min_periods=12)
    m1_cols = ["date", "actual_avg_bln", "required_avg_bln", "required_account_bln", "m1_reserve_spread_bln", "m1_reserve_spread_z_obs"]
    df = df.merge(asof_spread(calendar, m1[m1_cols], "m1_reserve"), on="date", how="left")

    ru = m1_ruonia.rename(columns={"ruonia": "m1_ruonia"})
    df = df.merge(asof_spread(calendar, ru, "m1_ruonia"), on="date", how="left")
    kr = keyrate.rename(columns={"keyrate": "m2_keyrate"})
    df = df.merge(asof_spread(calendar, kr, "keyrate"), on="date", how="left")
    df["m1_ruonia_keyrate_spread"] = df["m1_ruonia"] - df["m2_keyrate"]
    df["m1_ruonia_spread_z"] = robust_mad_z(df["m1_ruonia_keyrate_spread"], window=1095, min_periods=250)
    days_in_month = df["date"].dt.days_in_month
    df["m1_end_of_period_flag"] = ((days_in_month - df["date"].dt.day) <= 5).astype(int)

    # M2 repo. We do not have true demand, so use volume, utilization and rate spread proxy.
    # Use all repo terms to preserve history; 7-day auctions are dominant when they exist, but
    # in 2020-2024 other terms are the only available stress signal.
    auc = repo_auc[repo_auc["type"].astype(str).str.contains("Репо", case=False, na=False)].copy()
    if not auc.empty:
        auc["weighted_rate"] = auc["rate_wavg"] * auc["volume_bln"].fillna(0)
        auc["weighted_term"] = auc["term_days"] * auc["volume_bln"].fillna(0)
        auc = auc.groupby("date", as_index=False).agg(
            m2_repo_volume_bln=("volume_bln", "sum"),
            _weighted_rate=("weighted_rate", "sum"),
            _weighted_term=("weighted_term", "sum"),
            _volume=("volume_bln", "sum"),
        )
        auc["m2_repo_rate_wavg"] = np.where(auc["_volume"] > 0, auc["_weighted_rate"] / auc["_volume"], np.nan)
        auc["m2_repo_term_wavg"] = np.where(auc["_volume"] > 0, auc["_weighted_term"] / auc["_volume"], np.nan)
        auc = auc.drop(columns=["_weighted_rate", "_weighted_term", "_volume"])
    else:
        auc = pd.DataFrame(columns=["date", "m2_repo_volume_bln", "m2_repo_rate_wavg", "m2_repo_term_wavg"])

    par = repo_params.copy()
    if not par.empty:
        par = par.groupby("date", as_index=False).agg(
            m2_repo_limit_bln=("limit_bln", "sum"),
            m2_repo_min_rate=("min_rate", "min"),
        )
    else:
        par = pd.DataFrame(columns=["date", "m2_repo_limit_bln", "m2_repo_min_rate"])
    repo = pd.merge(auc, par, on="date", how="outer").sort_values("date")
    repo = pd.merge_asof(repo.sort_values("date"), keyrate.rename(columns={"keyrate": "m2_keyrate_on_repo"}).sort_values("date"), on="date", direction="backward")
    repo["m2_repo_utilization"] = repo["m2_repo_volume_bln"] / repo["m2_repo_limit_bln"].replace(0, np.nan)
    repo["m2_repo_rate_spread"] = repo["m2_repo_rate_wavg"] - repo["m2_keyrate_on_repo"]
    repo_event = repo[["date", "m2_repo_volume_bln", "m2_repo_rate_wavg", "m2_repo_term_wavg", "m2_repo_limit_bln", "m2_repo_min_rate", "m2_repo_utilization", "m2_repo_rate_spread"]]
    df = df.merge(asof_spread(calendar, repo_event, "m2_repo"), on="date", how="left")
    df["m2_repo_volume_z"] = robust_mad_z(df["m2_repo_volume_bln"], window=1095, min_periods=250)
    df["m2_repo_rate_spread_z"] = robust_mad_z(df["m2_repo_rate_spread"], window=1095, min_periods=250)
    df["m2_repo_utilization_z"] = robust_mad_z(df["m2_repo_utilization"], window=1095, min_periods=250)
    df["m2_repo_high_utilization_flag"] = (df["m2_repo_utilization"] > 0.95).astype(int)

    # M3 OFZ full history. Values are aggregated by auction date and spread only until max carry days.
    ofz = ofz.copy()
    ofz["yield_x_placement"] = ofz["avg_yield"] * ofz["placement_volume"].fillna(0)
    ofz_daily = ofz.groupby("date", as_index=False).agg(
        m3_offer_volume=("offer_volume", "sum"),
        m3_demand_volume=("demand_volume", "sum"),
        m3_placement_volume=("placement_volume", "sum"),
        _yield_sum=("yield_x_placement", "sum"),
        _placement_sum=("placement_volume", "sum"),
    )
    ofz_daily["m3_bid_cover"] = ofz_daily["m3_demand_volume"] / ofz_daily["m3_offer_volume"].replace(0, np.nan)
    ofz_daily["m3_demand_to_placement"] = ofz_daily["m3_demand_volume"] / ofz_daily["m3_placement_volume"].replace(0, np.nan)
    ofz_daily["m3_placement_share"] = ofz_daily["m3_placement_volume"] / ofz_daily["m3_offer_volume"].replace(0, np.nan)
    ofz_daily["m3_avg_yield"] = np.where(ofz_daily["_placement_sum"] > 0, ofz_daily["_yield_sum"] / ofz_daily["_placement_sum"], np.nan)
    ofz_daily = ofz_daily.drop(columns=["_yield_sum", "_placement_sum"])
    ofz_daily["m3_nedospros_flag"] = (ofz_daily["m3_bid_cover"] < 1.2).astype(int)
    ofz_daily["m3_perespros_flag"] = (ofz_daily["m3_bid_cover"] > 2.0).astype(int)
    ofz_daily["m3_nedospros_static_score"] = ((1.2 - ofz_daily["m3_bid_cover"]) / 1.2 * 4).clip(lower=0, upper=4)
    df = df.merge(asof_spread(calendar, ofz_daily, "m3_ofz"), on="date", how="left")
    df["m3_bid_cover_z"] = robust_mad_z(df["m3_bid_cover"], window=1095, min_periods=250)
    df["m3_avg_yield_z"] = robust_mad_z(df["m3_avg_yield"], window=1095, min_periods=250)
    df["m3_source_gap_flag"] = (df["m3_ofz_days_since_update"] > MAX_CARRY_DAYS["m3_ofz"]).astype(int)

    # M4 tax calendar as context/regime, not reconstructable market signal.
    tax_dates = sorted(pd.to_datetime(tax["date"].dropna().unique()))
    df["m4_tax_day_flag"] = df["date"].isin(tax_dates).astype(int)
    td = np.array(tax_dates, dtype="datetime64[D]") if len(tax_dates) else np.array([], dtype="datetime64[D]")
    all_dates = df["date"].values.astype("datetime64[D]")
    tax_week = np.zeros(len(df), dtype=int)
    days_to_tax = np.full(len(df), np.nan)
    days_since_tax = np.full(len(df), np.nan)
    if len(td):
        for i, d in enumerate(all_dates):
            diffs = (td - d).astype("timedelta64[D]").astype(int)
            if np.any(diffs >= 0):
                days_to_tax[i] = np.min(diffs[diffs >= 0])
            if np.any(diffs <= 0):
                days_since_tax[i] = -np.max(diffs[diffs <= 0])
            if np.any(np.abs(diffs) <= 5):
                tax_week[i] = 1
    df["m4_tax_week_flag"] = tax_week
    df["m4_days_to_tax"] = days_to_tax
    df["m4_days_since_tax"] = days_since_tax
    days_to_month_end = df["date"].dt.days_in_month - df["date"].dt.day
    df["m4_end_of_month_flag"] = (days_to_month_end <= 2).astype(int)
    df["m4_end_of_quarter_flag"] = ((df["date"].dt.month.isin([3, 6, 9, 12])) & (days_to_month_end <= 5)).astype(int)
    df["m4_seasonal_factor"] = (1.0 + 0.15 * df["m4_tax_week_flag"] + 0.05 * df["m4_end_of_month_flag"] + 0.10 * df["m4_end_of_quarter_flag"]).clip(1.0, 1.4)

    # M5 structural liquidity + monthly federal funds on bank accounts.
    bl = bliq.rename(columns={"structural_balance_bln": "m5_structural_balance_bln"})
    df = df.merge(asof_spread(calendar, bl, "m5_bliquidity"), on="date", how="left")
    df["m5_structural_balance_7d_delta"] = df["m5_structural_balance_bln"] - df["m5_structural_balance_bln"].shift(7)
    df["m5_structural_drain_7d"] = -df["m5_structural_balance_7d_delta"]
    df["m5_structural_drain_z"] = robust_mad_z(df["m5_structural_drain_7d"], window=1095, min_periods=250)

    fed = fed.rename(columns={"federal_funds_on_banks_bln": "m5_federal_funds_on_banks_bln"}).copy()
    fed["m5_federal_funds_mom_delta"] = fed["m5_federal_funds_on_banks_bln"].diff()
    fed["m5_federal_funds_mom_drain"] = -fed["m5_federal_funds_mom_delta"]
    fed["m5_federal_drain_z_obs"] = robust_mad_z(fed["m5_federal_funds_mom_drain"], window=36, min_periods=12)
    df = df.merge(asof_spread(calendar, fed, "m5_federal"), on="date", how="left")
    df["m5_budget_drain_flag"] = ((df["m5_structural_drain_7d"] > 300) | (df["m5_federal_funds_mom_drain"] > 300)).astype(int)

    # Availability: after daily spreading, a module is either valid today or excluded. No decay inside the allowed spread window.
    df["m1_available"] = df[["m1_reserve_spread_z_obs", "m1_ruonia_spread_z"]].notna().any(axis=1).astype(int)
    df["m2_available"] = df[["m2_repo_volume_z", "m2_repo_rate_spread_z", "m2_repo_utilization_z"]].notna().any(axis=1).astype(int)
    df["m3_available"] = df[["m3_bid_cover", "m3_bid_cover_z", "m3_nedospros_static_score"]].notna().any(axis=1).astype(int)
    df["m5_available"] = df[["m5_structural_drain_z", "m5_federal_drain_z_obs"]].notna().any(axis=1).astype(int)
    for m in ["m1", "m2", "m3", "m5"]:
        df[f"{m}_weight"] = df[f"{m}_available"].astype(float)
    base = {"m1_weight": 0.25, "m2_weight": 0.3333, "m3_weight": 0.25, "m5_weight": 0.1667}
    denom = sum(base.values())
    df["coverage_score"] = sum(df[k].fillna(0) * v for k, v in base.items()) / denom
    df["full_model_valid"] = ((df["m1_available"] == 1) & (df["m2_available"] == 1) & (df["m3_available"] == 1) & (df["m5_available"] == 1)).astype(int)

    df["crisis_window"] = "normal_or_unknown"
    for s, e, name in CRISIS_WINDOWS:
        mask = df["date"].between(pd.to_datetime(s), pd.to_datetime(e))
        df.loc[mask, "crisis_window"] = name

    return df


def status_from_lsi(x: float) -> str:
    if pd.isna(x):
        return "NA"
    if x < 40:
        return "green"
    if x < 70:
        return "yellow"
    return "red"


def make_backtest_summary(df: pd.DataFrame, lsi_col: str = "lsi") -> pd.DataFrame:
    rows = []
    for s, e, name in CRISIS_WINDOWS:
        start = pd.to_datetime(s)
        end = pd.to_datetime(e)
        w = df[df["date"].between(start, end)].copy()
        pre = df[df["date"].between(start - pd.Timedelta(days=14), start - pd.Timedelta(days=1))].copy()
        valid = w[w.get("full_model_valid", 0) == 1]
        if w.empty:
            rows.append({"window": name, "period": f"{s}..{e}", "note": "no rows in calendar"})
            continue
        if valid.empty:
            rows.append({
                "window": name,
                "period": f"{s}..{e}",
                "rows": len(w),
                "valid_rows": 0,
                "coverage_mean": round(float(w.get("coverage_score", pd.Series(dtype=float)).mean()), 3),
                "lsi_max": np.nan,
                "lsi_mean": np.nan,
                "pre_14d_lsi_max": np.nan,
                "red_days": 0,
                "yellow_or_red_days": 0,
                "note": "omitted: no full five-module coverage, mainly because OFZ is absent/stale",
            })
            continue
        rows.append({
            "window": name,
            "period": f"{s}..{e}",
            "rows": len(w),
            "valid_rows": len(valid),
            "coverage_mean": round(float(w["coverage_score"].mean()), 3),
            "lsi_max": round(float(valid[lsi_col].max()), 2),
            "lsi_mean": round(float(valid[lsi_col].mean()), 2),
            "pre_14d_lsi_max": round(float(pre.loc[pre.get("full_model_valid", 0) == 1, lsi_col].max()), 2) if not pre.empty else np.nan,
            "red_days": int((valid[lsi_col] >= 70).sum()),
            "yellow_or_red_days": int((valid[lsi_col] >= 40).sum()),
            "note": "full five-module backtest rows only",
        })
    return pd.DataFrame(rows)


def safe_to_csv(df: pd.DataFrame, path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
