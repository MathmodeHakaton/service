from src.presentation.data_loader import load_all
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


st.set_page_config(page_title="М1 · Резервы", page_icon="📊", layout="wide")
st.title("М1 — Усреднение обязательных резервов + RUONIA")

with st.spinner("Загрузка..."):
    r1, r2, r3, r4, r5, lsi = load_all()

df1 = r1["signals_df"].dropna(
    subset=["MAD_score_RUONIA"]) if not r1["signals_df"].empty else pd.DataFrame()
st.caption("Источник: Банк России — обязательные резервы и RUONIA (с 2014)")

if df1.empty or "spread" not in df1.columns:
    st.info("Нет данных")
    st.stop()

# ── Выбор периода ─────────────────────────────────────────────────────────
st.markdown("### Выберите период")
col_btns = st.columns(5)
presets = {"6 месяцев": 180, "1 год": 365,
           "3 года": 1095, "5 лет": 1825, "Всё": None}
selected = st.session_state.get("m1_preset", "3 года")

for i, (label, days) in enumerate(presets.items()):
    with col_btns[i]:
        if st.button(label, use_container_width=True,
                     type="primary" if label == selected else "secondary", key=f"m1_{label}"):
            st.session_state["m1_preset"] = label
            selected = label

date_max = pd.Timestamp(df1["date"].max())
date_min = pd.Timestamp(df1["date"].min())
preset_days = presets[selected]
date_start = date_min if preset_days is None else max(
    date_min, date_max - pd.Timedelta(days=preset_days))

col_d1, col_d2 = st.columns(2)
with col_d1:
    d_from = st.date_input("От", value=date_start.date(),
                           min_value=date_min.date(), max_value=date_max.date(), key="m1_from")
with col_d2:
    d_to = st.date_input("До", value=date_max.date(),
                         min_value=date_min.date(), max_value=date_max.date(), key="m1_to")

df1_f = df1[(df1["date"] >= pd.Timestamp(d_from)) &
            (df1["date"] <= pd.Timestamp(d_to))].copy()

# ── График ────────────────────────────────────────────────────────────────
fig1 = go.Figure()
fig1.add_trace(go.Bar(
    x=df1_f["date"], y=df1_f["spread"].fillna(0),
    name="Запас резервов (факт − норматив), млрд руб.",
    marker_color="steelblue", opacity=0.55,
))
if "ruonia_avg" in df1_f.columns:
    fig1.add_trace(go.Scatter(
        x=df1_f["date"], y=df1_f["ruonia_avg"],
        name="Ставка RUONIA, % год.",
        yaxis="y2", line=dict(color="crimson", width=2),
    ))
fig1.update_layout(
    title=f"М1: Запас резервов и ставка RUONIA  ({d_from} — {d_to})",
    yaxis=dict(title="Запас, млрд руб."),
    yaxis2=dict(title="RUONIA, %", overlaying="y", side="right"),
    height=400, legend=dict(x=0, y=1.12, orientation="h"),
)
st.plotly_chart(fig1, use_container_width=True)
