from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch

CRISIS_WINDOWS = [
    ("2014-12-01", "2015-01-31", "dec 2014"),
    ("2022-02-01", "2022-04-30", "feb-mar 2022"),
    ("2023-08-01", "2023-09-30", "aug 2023"),
]

GREEN = "#27ae60"
YELLOW = "#f39c12"
RED = "#e74c3c"
GREY = "#bdbdbd"
BLUE = "#1f4e8a"        # deeper, more "bank-finance" blue for main LSI line
BLUE_FAINT = "#7fa8d6"  # daily raw

# Known events to annotate (bank-side recognisable inflection points).
KEY_EVENTS = [
    ("2014-12-16", "Black Tuesday\nRUB crash"),
    ("2022-02-24", "Feb 24, 2022\nsanctions shock"),
    ("2023-08-15", "Aug 2023\nRUB depreciation"),
    ("2024-10-25", "Key rate → 21%"),
]


def _ema(s: pd.Series, span: int = 7) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def _shade_zones(ax, xmin=None, xmax=None) -> None:
    """Gradient-style background zones with explicit zone labels on the right."""
    ax.axhspan(0, 40, color=GREEN, alpha=0.10, zorder=0)
    ax.axhspan(40, 55, color="#f5e88a", alpha=0.18, zorder=0)
    ax.axhspan(55, 70, color=YELLOW, alpha=0.22, zorder=0)
    ax.axhspan(70, 85, color="#f0876a", alpha=0.22, zorder=0)
    ax.axhspan(85, 100, color=RED, alpha=0.28, zorder=0)
    for y in (40, 70, 85):
        ax.axhline(y, color="#555", lw=0.7, ls="--", zorder=1)
    # Right-edge zone labels.
    if xmax is not None:
        for y, txt, color in [(20, "GREEN  normal", "#1e6b3a"),
                              (55, "YELLOW  watch", "#a8650a"),
                              (78, "RED  stress", "#a01f1f"),
                              (93, "DEEP RED  crisis", "#6e0000")]:
            ax.text(xmax, y, "  " + txt, fontsize=9, va="center", ha="left",
                    color=color, fontweight="bold")


def _shade_crises(ax, xmin, xmax) -> None:
    for start, end, label in CRISIS_WINDOWS:
        s = pd.Timestamp(start)
        e = pd.Timestamp(end)
        if e < xmin or s > xmax:
            continue
        ax.axvspan(max(s, xmin), min(e, xmax), color="#000", alpha=0.06, zorder=0)
        ax.text(max(s, xmin), 98, f" {label}", fontsize=8, color="#555", va="top")


def _shade_partial(ax, df: pd.DataFrame) -> None:
    """Grey-shade ranges where full_model_valid == 0 (not all modules covered)."""
    if "full_model_valid" not in df.columns:
        return
    invalid = df["full_model_valid"].fillna(1).astype(int).eq(0).values
    if not invalid.any():
        return
    dates = df["date"].values
    in_block = False
    start_idx = 0
    for i, flag in enumerate(invalid):
        if flag and not in_block:
            in_block = True
            start_idx = i
        elif not flag and in_block:
            in_block = False
            ax.axvspan(dates[start_idx], dates[i - 1], color=GREY, alpha=0.18, zorder=0)
    if in_block:
        ax.axvspan(dates[start_idx], dates[-1], color=GREY, alpha=0.18, zorder=0)


def plot_lsi_timeseries(df: pd.DataFrame, out_path: Path, title_suffix: str = "") -> Path:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    raw_col = next((c for c in ("lsi_raw", "lsi_gated_raw", "catboost_lsi_raw") if c in df.columns), None)
    if raw_col is None:
        raise ValueError("No LSI raw column found in DataFrame")
    if "lsi_smoothed" not in df.columns:
        df["lsi_smoothed"] = _ema(df[raw_col], span=7)

    # Weekly aggregate of the Kalman-smoothed line — the bold "executive view" curve.
    weekly = (df.set_index("date")["lsi_smoothed"]
                .resample("W").mean()
                .reindex(pd.date_range(df["date"].min(), df["date"].max(), freq="D"))
                .interpolate(method="time"))

    xmin, xmax = df["date"].min(), df["date"].max()
    fig, ax = plt.subplots(figsize=(16, 7.2))
    _shade_zones(ax, xmin, xmax)
    _shade_partial(ax, df)
    _shade_crises(ax, xmin, xmax)

    # Three layers: raw (very faint), Kalman daily (medium), weekly (bold headline).
    ax.plot(df["date"], df[raw_col], color=BLUE_FAINT, lw=0.5, alpha=0.30,
            label="daily gating prediction")
    ax.plot(df["date"], df["lsi_smoothed"], color=BLUE, lw=1.0, alpha=0.55,
            label="Kalman-smoothed (daily)")
    ax.plot(weekly.index, weekly.values, color=BLUE, lw=2.8, label="LSI (weekly headline)")
    # Fill between weekly LSI and zero with light blue for visual weight.
    ax.fill_between(weekly.index, 0, weekly.values, color=BLUE, alpha=0.06, zorder=1)

    # Event annotations.
    for ev_date, ev_label in KEY_EVENTS:
        d = pd.Timestamp(ev_date)
        if d < xmin or d > xmax:
            continue
        # find smoothed value at that date
        idx = (df["date"] - d).abs().idxmin()
        y = float(df.loc[idx, "lsi_smoothed"])
        ax.axvline(d, color="#444", lw=0.6, ls=":", alpha=0.6, zorder=2)
        ax.annotate(ev_label, xy=(d, y), xytext=(0, 14),
                    textcoords="offset points", fontsize=8, ha="center",
                    bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="#888", lw=0.5, alpha=0.9),
                    arrowprops=dict(arrowstyle="-", color="#888", lw=0.6))

    # Current state badge in upper-left.
    last = df.iloc[-1]
    headline = float(last.get("lsi_smoothed", last[raw_col]))
    status = str(last.get("status", "?")).upper()
    badge_color = {"GREEN": GREEN, "YELLOW": YELLOW, "RED": RED, "PARTIAL": GREY}.get(status, "#444")
    badge_text = f"  Current ({last['date'].date()}) \n  LSI = {headline:.1f}  •  {status}  "
    ax.text(0.012, 0.965, badge_text, transform=ax.transAxes, fontsize=11, fontweight="bold",
            color="white", va="top", ha="left",
            bbox=dict(boxstyle="round,pad=0.5", fc=badge_color, ec="none", alpha=0.92))
    ax.scatter([last["date"]], [headline], color=badge_color, edgecolor="white", lw=1.5, zorder=10, s=80)

    ax.set_ylim(0, 100)
    ax.set_xlim(xmin, xmax + pd.Timedelta(days=120))  # leave room for zone labels on the right
    ax.set_ylabel("LSI (0–100)", fontsize=11)
    ax.set_xlabel("")
    ax.set_title(f"PSB Liquidity Stress Index{title_suffix}",
                 fontsize=14, fontweight="bold", loc="left", pad=12)
    ax.text(0.0, 1.005, "Daily forecast • Kalman-smoothed • bank-grade thermometer",
            transform=ax.transAxes, fontsize=9, color="#666", ha="left", va="bottom")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.tick_params(axis="x", labelsize=9)
    ax.tick_params(axis="y", labelsize=9)
    ax.grid(True, axis="y", alpha=0.18, linestyle="-", linewidth=0.5)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)

    legend_extra = [Patch(facecolor=GREY, alpha=0.4, label="partial coverage"),
                    Patch(facecolor="#000", alpha=0.12, label="known crisis window")]
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles + legend_extra, labels + [p.get_label() for p in legend_extra],
              loc="upper right", fontsize=8.5, ncol=1, framealpha=0.92,
              bbox_to_anchor=(0.985, 0.97))

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return out_path


def plot_shap_module_contributions(df: pd.DataFrame, out_path: Path) -> Path:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    shap_cols = [c for c in ["shap_M1", "shap_M2", "shap_M3", "shap_M4", "shap_M5",
                             "shap_COHERENCE", "shap_INTERACTION"] if c in df.columns]
    if not shap_cols:
        return out_path
    pos = df[shap_cols].clip(lower=0).fillna(0)
    neg = df[shap_cols].clip(upper=0).fillna(0)

    fig, ax = plt.subplots(figsize=(15, 6))
    colors = plt.cm.tab10(np.linspace(0, 1, len(shap_cols)))
    ax.stackplot(df["date"], pos.T.values, labels=[c.replace("shap_", "") for c in shap_cols],
                 colors=colors, alpha=0.85)
    ax.stackplot(df["date"], neg.T.values, colors=colors, alpha=0.55)
    baseline = df["shap_expected_value"].iloc[0] if "shap_expected_value" in df.columns else 0
    ax.axhline(0, color="#444", lw=0.6)
    ax.set_title(f"SHAP module contributions per day (baseline ≈ {baseline:.1f})")
    ax.set_ylabel("contribution to LSI")
    ax.set_xlabel("date")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.legend(loc="upper left", ncol=4, fontsize=8)
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


def plot_signed_module_balance(df: pd.DataFrame, out_path: Path) -> Path:
    """Stacked bar of signed module signals (positive = stress, negative = calm)."""
    net_cols = [c for c in ["m1_net_signal", "m2_net_signal", "m3_net_signal", "m5_net_signal"] if c in df.columns]
    if not net_cols:
        return out_path
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Weekly resample to keep the chart readable across ~10 years.
    weekly = df.set_index("date")[net_cols + (["net_market_signal"] if "net_market_signal" in df.columns else [])].resample("W").mean()

    fig, ax = plt.subplots(figsize=(15, 5.5))
    colors = {"m1_net_signal": "#9467bd", "m2_net_signal": "#1f77b4",
              "m3_net_signal": "#2ca02c", "m5_net_signal": "#d62728"}
    pos = weekly[net_cols].clip(lower=0)
    neg = weekly[net_cols].clip(upper=0)
    bottom_pos = np.zeros(len(weekly))
    bottom_neg = np.zeros(len(weekly))
    width = 5.5  # ~weekly width in days
    for c in net_cols:
        ax.bar(weekly.index, pos[c].values, bottom=bottom_pos, width=width,
               color=colors.get(c, "#888"), label=c.replace("_net_signal", "").upper(), alpha=0.85)
        bottom_pos += pos[c].values
        ax.bar(weekly.index, neg[c].values, bottom=bottom_neg, width=width,
               color=colors.get(c, "#888"), alpha=0.55)
        bottom_neg += neg[c].values
    if "net_market_signal" in weekly.columns:
        ax.plot(weekly.index, weekly["net_market_signal"], color="#000", lw=1.4, label="net market signal")
    ax.axhline(0, color="#444", lw=0.6)
    ax.set_title("Signed module balance (weekly mean): positive = stress, negative = calm evidence")
    ax.set_ylabel("net z-units")
    ax.set_xlabel("date")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(loc="upper left", ncol=5, fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


MODULE_COLORS = {
    "M1": "#9467bd",   # purple
    "M2": "#1f77b4",   # blue
    "M3": "#2ca02c",   # green
    "M4": "#7f7f7f",   # grey (calendar)
    "M5": "#d62728",   # red
}


def plot_dynamic_weights(df: pd.DataFrame, out_path: Path) -> Path:
    """Stacked area: dynamic per-day weights alpha_i(t). Sum = 1 by construction."""
    alpha_cols = [f"alpha_M{i}" for i in range(1, 6) if f"alpha_M{i}" in df.columns]
    if not alpha_cols:
        return out_path
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Smooth alphas weekly to avoid visual noise.
    weekly = df.set_index("date")[alpha_cols].resample("W").mean()

    fig, ax = plt.subplots(figsize=(16, 4.5))
    colors = [MODULE_COLORS.get(c.replace("alpha_", ""), "#888") for c in alpha_cols]
    labels = [c.replace("alpha_", "") for c in alpha_cols]
    ax.stackplot(weekly.index, weekly[alpha_cols].T.values, labels=labels, colors=colors, alpha=0.92)
    ax.set_ylim(0, 1)
    ax.set_ylabel("module weight α")
    ax.set_xlabel("")
    ax.set_title("Dynamic module weights α(t) — gating model output (weekly mean)",
                 fontsize=12, fontweight="bold", loc="left")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.grid(True, axis="y", alpha=0.20)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.legend(loc="upper right", ncol=5, fontsize=9, framealpha=0.92)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return out_path


def plot_module_contributions(df: pd.DataFrame, out_path: Path) -> Path:
    """Per-module SHAP contributions to LSI, with baseline expected_value at the bottom.

    By SHAP identity:
        LSI(t) = baseline_expected(t) + Σ contribution_M{i}(t)
    Positive bars = the module pushed LSI up that week; negative bars = it pulled LSI down.
    """
    contrib_cols = [f"contribution_M{i}" for i in (1, 2, 3, 4, 5) if f"contribution_M{i}" in df.columns]
    if not contrib_cols:
        return out_path
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    weekly = df.set_index("date")[contrib_cols + (["lsi_baseline_expected"] if "lsi_baseline_expected" in df.columns else [])].resample("W").mean()
    contrib = weekly[contrib_cols]
    pos = contrib.clip(lower=0)
    neg = contrib.clip(upper=0)

    fig, ax = plt.subplots(figsize=(16, 6))
    colors = {c: MODULE_COLORS.get(c.replace("contribution_", ""), "#888") for c in contrib_cols}
    # Positive stack starts from baseline; negative stack drops below.
    baseline = weekly.get("lsi_baseline_expected", pd.Series(0.0, index=weekly.index))
    ax.fill_between(weekly.index, 0, baseline, color="#cccccc", alpha=0.6, label="baseline (expected)")
    bottom_pos = baseline.values.astype(float).copy()
    bottom_neg = np.zeros(len(weekly))
    for c in contrib_cols:
        ax.fill_between(weekly.index, bottom_pos, bottom_pos + pos[c].values,
                        color=colors[c], alpha=0.85, label=c.replace("contribution_", ""))
        bottom_pos += pos[c].values
        ax.fill_between(weekly.index, bottom_neg, bottom_neg + neg[c].values,
                        color=colors[c], alpha=0.55)
        bottom_neg += neg[c].values
    ax.axhline(0, color="#444", lw=0.6)
    ax.set_ylabel("LSI contribution")
    ax.set_xlabel("")
    ax.set_title("LSI decomposition via SHAP: baseline + per-module contributions (weekly mean)",
                 fontsize=12, fontweight="bold", loc="left")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    for y in (40, 70):
        ax.axhline(y, color="#555", lw=0.7, ls="--", alpha=0.4)
    ax.grid(True, axis="y", alpha=0.20)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.legend(loc="upper left", ncol=6, fontsize=9, framealpha=0.92)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return out_path


def plot_active_modules(df: pd.DataFrame, out_path: Path) -> Path:
    if "active_market_modules_count" not in df.columns:
        return out_path
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    fig, ax = plt.subplots(figsize=(15, 3.2))
    ax.fill_between(df["date"], 0, df["active_market_modules_count"],
                    color="#1f77b4", alpha=0.55, step="pre")
    ax.set_ylim(0, 5)
    ax.set_ylabel("active modules")
    ax.set_title("Active market modules per day (0..5)")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


def plot_dashboard(df: pd.DataFrame, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    paths.append(plot_lsi_timeseries(df, out_dir / "lsi_timeseries.png"))
    paths.append(plot_module_contributions(df, out_dir / "module_contributions.png"))
    paths.append(plot_active_modules(df, out_dir / "active_modules.png"))
    last_2y_cutoff = pd.to_datetime(df["date"]).max() - pd.Timedelta(days=730)
    recent = df[pd.to_datetime(df["date"]) >= last_2y_cutoff]
    if len(recent) > 30:
        paths.append(plot_lsi_timeseries(recent, out_dir / "lsi_timeseries_last2y.png",
                                         title_suffix=" — last 2 years"))
    return paths
