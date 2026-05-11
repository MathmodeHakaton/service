from src.presentation.data_loader import load_all
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


st.set_page_config(page_title="М3 · ОФЗ", page_icon="📊", layout="wide")
st.title("М3 — Размещение ОФЗ Минфина")

with st.spinner("Загрузка..."):
    r1, r2, r3, r4, r5, lsi = load_all()

df3 = r3["signals_df"]
st.caption(
    "Источник: Министерство финансов РФ — результаты аукционов ОФЗ (2016–2026)")

if df3 is None or df3.empty or "cover_ratio" not in df3.columns:
    st.info("Нет данных ОФЗ — нужны результаты аукционов Минфина")
    st.stop()

# ── Выбор периода ─────────────────────────────────────────────────────────
st.markdown("### Выберите период")
col_btns = st.columns(5)
presets = {"6 месяцев": 180, "1 год": 365,
           "3 года": 1095, "5 лет": 1825, "Всё": None}
selected = st.session_state.get("m3_preset", "3 года")

for i, (label, days) in enumerate(presets.items()):
    with col_btns[i]:
        if st.button(label, use_container_width=True,
                     type="primary" if label == selected else "secondary", key=f"m3_{label}"):
            st.session_state["m3_preset"] = label
            selected = label

date_max = pd.Timestamp(df3["date"].max())
date_min = pd.Timestamp(df3["date"].min())
preset_days = presets[selected]
date_start = date_min if preset_days is None else max(
    date_min, date_max - pd.Timedelta(days=preset_days))

col_d1, col_d2 = st.columns(2)
with col_d1:
    d_from = st.date_input("От", value=date_start.date(),
                           min_value=date_min.date(), max_value=date_max.date(), key="m3_from")
with col_d2:
    d_to = st.date_input("До", value=date_max.date(),
                         min_value=date_min.date(), max_value=date_max.date(), key="m3_to")

auctions3 = df3[
    (df3["date"] >= pd.Timestamp(d_from)) & (df3["date"] <= pd.Timestamp(d_to))
].dropna(subset=["cover_ratio"]).copy().reset_index(drop=True)

# ── График ────────────────────────────────────────────────────────────────
if len(auctions3):
    auctions3["rank"] = auctions3.groupby("date").cumcount()
    auctions3["n_per_day"] = auctions3.groupby(
        "date")["rank"].transform("count")
    auctions3["x_offset"] = auctions3.apply(
        lambda r: pd.Timedelta(days=-0.3) if r["n_per_day"] > 1 and r["rank"] == 0
        else (pd.Timedelta(days=0.3) if r["n_per_day"] > 1 and r["rank"] == 1
              else pd.Timedelta(0)), axis=1
    )
    auctions3["x"] = auctions3["date"] + auctions3["x_offset"]
    colors3 = [
        "crimson" if c < 1.2 else
        "steelblue" if c > 2.0 else "#aaaaaa"
        for c in auctions3["cover_ratio"]
    ]
    fig3 = go.Figure()
    fig3.add_trace(go.Bar(
        x=auctions3["x"], y=auctions3["cover_ratio"],
        marker_color=colors3, showlegend=False,
        width=[1000 * 3600 * 24 * 0.55] * len(auctions3),
    ))
    fig3.add_hline(y=1.2, line_dash="dash", line_color="orange", line_width=1.5,
                   annotation_text="< 1.2 недоспрос", annotation_position="right")
    fig3.add_hline(y=2.0, line_dash="dash", line_color="steelblue", line_width=1.5,
                   annotation_text="> 2.0 переспрос", annotation_position="right")
    fig3.update_layout(
        title=f"М3: Cover ratio аукционов ОФЗ  ({d_from} — {d_to})",
        yaxis_title="cover_ratio", xaxis=dict(type="date"), height=400,
    )
    st.plotly_chart(fig3, use_container_width=True)
    st.markdown(
        "🔴 Недоспрос (< 1.2) &nbsp;&nbsp; 🔵 Переспрос (> 2.0) &nbsp;&nbsp; ⚪ Норма (1.2 – 2.0)")
    st.caption("Два столбика на дату = два выпуска ОФЗ в один день.")
else:
    st.info("Нет аукционов за выбранный период")
