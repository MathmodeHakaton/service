from src.presentation.data_loader import load_all
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


st.set_page_config(page_title="М5 · Казначейство",
                   page_icon="📊", layout="wide")
st.title("М5 — Средства федерального казначейства")

with st.spinner("Загрузка..."):
    r1, r2, r3, r4, r5, lsi = load_all()

scores = lsi["scores"]
df5 = r5["signals_df"]

st.caption(
    "Источник: Банк России — таблица дефицита/профицита ликвидности (с 2014)")

if df5.empty or "balance" not in df5.columns:
    st.info("Нет данных")
    st.stop()

# ── Выбор периода ─────────────────────────────────────────────────────────
st.markdown("### Выберите период")
col_btns = st.columns(5)
presets = {"6 месяцев": 180, "1 год": 365,
           "3 года": 1095, "5 лет": 1825, "Всё": None}
selected = st.session_state.get("m5_preset", "3 года")

for i, (label, days) in enumerate(presets.items()):
    with col_btns[i]:
        if st.button(label, use_container_width=True,
                     type="primary" if label == selected else "secondary", key=f"m5_{label}"):
            st.session_state["m5_preset"] = label
            selected = label

date_max = pd.Timestamp(df5["date"].max())
date_min = pd.Timestamp(df5["date"].min())
preset_days = presets[selected]
date_start = date_min if preset_days is None else max(
    date_min, date_max - pd.Timedelta(days=preset_days))

col_d1, col_d2 = st.columns(2)
with col_d1:
    d_from = st.date_input("От", value=date_start.date(),
                           min_value=date_min.date(), max_value=date_max.date(), key="m5_from")
with col_d2:
    d_to = st.date_input("До", value=date_max.date(),
                         min_value=date_min.date(), max_value=date_max.date(), key="m5_to")

df5 = df5[(df5["date"] >= pd.Timestamp(d_from)) &
          (df5["date"] <= pd.Timestamp(d_to))].copy()

# Инвертируем знак: профицит сверху (+), дефицит снизу (−)
df5["balance_inv"] = -df5["balance"]
bal_last = df5["balance_inv"].iloc[-1]

fig5 = go.Figure()
fig5.add_trace(go.Scatter(
    x=df5["date"], y=df5["balance_inv"],
    fill="tozeroy",
    fillcolor="rgba(39,174,96,0.15)" if bal_last > 0 else "rgba(231,76,60,0.15)",
    line=dict(color="steelblue", width=1.8),
    name="Структурный баланс, млрд руб.",
))

if "Flag_Budget_Drain" in df5.columns:
    drains = df5[df5["Flag_Budget_Drain"] == 1]
    if len(drains):
        fig5.add_trace(go.Scatter(
            x=drains["date"], y=drains["balance_inv"], mode="markers",
            name="⚠ Flag_Budget_Drain (пик оттока > 500 млрд/нед)",
            marker=dict(color="red", size=8, symbol="triangle-down"),
        ))

fig5.add_hline(y=0, line_color="black", line_width=1,
               annotation_text="граница профицит/дефицит")
fig5.update_layout(
    title="М5: Структурный дефицит/профицит ликвидности",
    yaxis_title="млрд руб.",
    height=400,
    legend=dict(x=0, y=1.12, orientation="h"),
)
st.plotly_chart(fig5, use_container_width=True)
st.caption(
    "Выше нуля = профицит (деньги с избытком). Ниже нуля = дефицит (банки занимают у ЦБ).")
