from __future__ import annotations
import streamlit as st
import sys
from pathlib import Path
from datetime import timedelta
import pandas as pd
import plotly.graph_objects as go
from src.presentation.llm_commentary import (
    build_prompt, generate_commentary, llm_available, LLM_MODEL,
)
"""
LSI · Liquidity Stress Index — финальный агрегационный слой.

Источник данных: предобученная CatBoost-модель из
data/model_artifacts/* (выгружено из проекта psb_catboost_weak_target_project).
Графики строятся по выгруженным CSV (lsi_timeseries / lsi_dashboard_extract /
module_importance_catboost / feature_importance / backtest_crisis_episodes).

LLM-комментарий: квантованный Qwen через Ollama
(модель qwen2.5:3b-instruct по умолчанию, переопределяется в settings).
"""

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


st.set_page_config(page_title="LSI · Агрегация", page_icon="📊", layout="wide")

ROOT = Path(__file__).resolve().parents[3]
ART = ROOT / "data" / "model_artifacts"

MODULE_LABELS = {
    "M1": "М1 · Резервы",
    "M2": "М2 · Репо ЦБ",
    "M3": "М3 · ОФЗ",
    "M4": "М4 · Сезонность",
    "M5": "М5 · Казначейство",
}
STATUS_COLOR = {"green": "#27ae60", "yellow": "#f39c12", "red": "#e74c3c",
                "partial": "#95a5a6"}
STATUS_RU = {"green": "🟢 НОРМА", "yellow": "🟡 ВНИМАНИЕ",
             "red": "🔴 СТРЕСС", "partial": "⚪ ЧАСТИЧНО"}


# ДАННЫЕ
@st.cache_data(ttl=3600)
def load_artifacts():
    extract = pd.read_csv(
        ART / "lsi_dashboard_extract.csv", parse_dates=["date"])
    mod_imp = pd.read_csv(ART / "module_importance_catboost.csv")
    feat_imp = pd.read_csv(ART / "feature_importance.csv")
    backtest = pd.read_csv(ART / "backtest_crisis_episodes.csv")
    return extract, mod_imp, feat_imp, backtest


@st.cache_data(ttl=3600)
def load_tax_calendar():
    """Налоговый календарь из основного pipeline (для блока «ближайшие события»)."""
    try:
        from src.application.pipeline import Pipeline
        from src.infrastructure.storage.db.engine import get_session
        s = get_session()
        try:
            p = Pipeline(session=s)
            data = p.execute_full().raw_data
            return data.get("tax_calendar", pd.DataFrame())
        finally:
            s.close()
    except Exception:
        return pd.DataFrame()


try:
    extract, mod_imp, feat_imp, backtest = load_artifacts()
except FileNotFoundError as e:
    st.error(f"Не найдены артефакты модели: {e}. "
             f"Ожидаются в {ART}.")
    st.stop()


# ШАПКА

st.title("LSI — Liquidity Stress Index")
st.caption("Агрегационный слой CatBoost · SHAP-интерпретация вклада модулей · "
           "Источники: ЦБ РФ, Минфин, Росказна, ФНС")

valid = extract[extract["full_model_valid"] == 1]
latest = (valid.iloc[-1] if not valid.empty else extract.iloc[-1])
latest_date = pd.to_datetime(latest["date"]).date()

contributions = {m: float(latest.get(f"contribution_{m}", 0.0) or 0.0)
                 for m in ["M1", "M2", "M3", "M4", "M5"]}
lsi_val = float(latest.get("lsi") if pd.notna(
    latest.get("lsi")) else latest["lsi_smoothed"])
lsi_raw = float(latest["lsi_raw"])
status = str(latest["status"])

col_g, col_s = st.columns([2, 1])
with col_g:
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=lsi_val,
        title={"text": f"LSI на {latest_date.isoformat()}", "font": {
            "size": 14}},
        gauge={
            "axis": {"range": [0, 100],
                     "tickvals": [0, 40, 70, 100],
                     "ticktext": ["0", "40", "70", "100"]},
            "bar": {"color": "#2c3e50"},
            "steps": [
                {"range": [0, 40], "color": "#d5f5e3"},
                {"range": [40, 70], "color": "#fdebd0"},
                {"range": [70, 100], "color": "#fadbd8"},
            ],
            "threshold": {"line": {"color": "black", "width": 3}, "value": lsi_val},
        },
        number={"suffix": " / 100", "font": {"size": 32}},
    ))
    fig.update_layout(height=260, margin=dict(t=30, b=10, l=10, r=10))
    st.plotly_chart(fig, width='stretch')

with col_s:
    color = STATUS_COLOR.get(status, "#95a5a6")
    st.markdown(f"""
    <div style="background:{color};color:white;padding:32px 16px;
                border-radius:12px;text-align:center;margin-top:30px;">
        <div style="font-size:22px;font-weight:bold;">{STATUS_RU.get(status, status)}</div>
        <div style="font-size:48px;font-weight:bold;margin:8px 0;">{lsi_val:.1f}</div>
        <div style="font-size:12px;opacity:0.85;">LSI сглаженный (Калман+гистерезис)</div>
        <div style="font-size:11px;opacity:0.7;margin-top:6px;">сырой: {lsi_raw:.1f} · SF×{latest['m4_multiplier']:.2f}</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# ВКЛАД МОДУЛЕЙ (SHAP)
st.subheader("Вклад модулей в текущий LSI (SHAP)")
st.caption("SHAP-разложение прогноза CatBoost: сумма вкладов + baseline = LSI. "
           "Положительный вклад тянет вверх (к стрессу), отрицательный — вниз.")

contrib_df = pd.DataFrame({
    "module": [MODULE_LABELS[m] for m in ["M1", "M2", "M3", "M4", "M5"]],
    "contribution": [contributions[m] for m in ["M1", "M2", "M3", "M4", "M5"]],
})
contrib_df = contrib_df.sort_values(
    "contribution", key=lambda s: s.abs(), ascending=False)
colors = ["#e74c3c" if v > 0 else "#27ae60" for v in contrib_df["contribution"]]

fig_c = go.Figure(go.Bar(
    x=contrib_df["contribution"], y=contrib_df["module"],
    orientation="h", marker_color=colors,
    text=[f"{v:+.2f}" for v in contrib_df["contribution"]], textposition="outside",
))
fig_c.add_vline(x=0, line_color="gray", line_dash="dot")
fig_c.update_layout(height=300, margin=dict(t=10, b=10, l=10, r=20),
                    xaxis_title="Вклад в LSI (пунктов)")
st.plotly_chart(fig_c, width='stretch')

# LSI TIMESERIES
st.subheader("История LSI")

ts = extract.dropna(subset=["lsi_smoothed"]).copy()
ts["status"] = ts["status"].astype(str)

fig_ts = go.Figure()
for st_key, color in STATUS_COLOR.items():
    sub = ts[ts["status"] == st_key]
    if not sub.empty:
        fig_ts.add_trace(go.Scatter(
            x=sub["date"], y=sub["lsi_smoothed"], mode="markers",
            name=STATUS_RU.get(st_key, st_key),
            marker=dict(color=color, size=3, opacity=0.55),
        ))
fig_ts.add_trace(go.Scatter(
    x=ts["date"], y=ts["lsi_smoothed"], mode="lines",
    name="LSI (Kalman)", line=dict(color="#2c3e50", width=1.2),
    showlegend=False,
))
for s, e, lbl in [("2022-02-01", "2022-04-30", "Фев-апр 2022"),
                  ("2023-08-01", "2023-09-30", "Авг-сен 2023")]:
    fig_ts.add_vrect(x0=s, x1=e, fillcolor="red", opacity=0.07,
                     annotation_text=lbl, annotation_position="top left",
                     annotation_font_size=10)
fig_ts.add_hline(y=40, line_dash="dash", line_color="#27ae60")
fig_ts.add_hline(y=70, line_dash="dash", line_color="#e74c3c")
fig_ts.update_layout(height=420, yaxis=dict(range=[0, 105], title="LSI"),
                     xaxis_title="Дата", legend=dict(x=0, y=1.12, orientation="h"))
st.plotly_chart(fig_ts, width='stretch')
