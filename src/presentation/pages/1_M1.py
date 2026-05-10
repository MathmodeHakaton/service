import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

st.set_page_config(page_title="М1 · Резервы", page_icon="📊", layout="wide")
st.title("М1 — Усреднение обязательных резервов + RUONIA")

from src.presentation.data_loader import load_all
with st.spinner("Загрузка..."):
    r1, r2, r3, r4, r5, lsi = load_all()

scores = lsi["scores"]
df1 = r1["signals_df"].dropna(subset=["MAD_score_RUONIA"]) if not r1["signals_df"].empty else pd.DataFrame()

st.metric("Стресс-оценка М1", f"{scores.get('M1', 0):.1f} / 100")
st.caption("Источник: Банк России — обязательные резервы (с 2004) и RUONIA (с 2014)")

if not df1.empty and "spread" in df1.columns:
    fig1 = go.Figure()
    fig1.add_trace(go.Bar(
        x=df1["date"], y=df1["spread"].fillna(0),
        name="Запас резервов (факт − норматив), млрд руб.",
        marker_color="steelblue", opacity=0.55,
    ))
    if "ruonia_avg" in df1.columns:
        fig1.add_trace(go.Scatter(
            x=df1["date"], y=df1["ruonia_avg"],
            name="Ставка RUONIA, % год.",
            yaxis="y2", line=dict(color="crimson", width=2),
        ))
    fig1.update_layout(
        title="М1: Запас резервов и ставка RUONIA",
        yaxis=dict(title="Запас, млрд руб."),
        yaxis2=dict(title="RUONIA, %", overlaying="y", side="right"),
        height=360, legend=dict(x=0, y=1.12, orientation="h"),
    )
    st.plotly_chart(fig1, use_container_width=True)

if not df1.empty:
    fig1b = go.Figure()
    fig1b.add_trace(go.Scatter(
        x=df1["date"], y=df1["MAD_score_RUONIA"],
        name="MAD_score_RUONIA — аномалия ставки межбанка",
        line=dict(color="crimson", width=1.8),
    ))
    if "MAD_score_спред" in df1.columns:
        fig1b.add_trace(go.Scatter(
            x=df1["date"], y=df1["MAD_score_спред"],
            name="MAD_score_спред — аномалия запаса резервов",
            line=dict(color="steelblue", width=1.5),
        ))
    if "Flag_AboveKey" in df1.columns:
        above = df1[df1["Flag_AboveKey"] == 1]
        if len(above):
            fig1b.add_trace(go.Scatter(
                x=above["date"], y=above["MAD_score_RUONIA"], mode="markers",
                name="⚠ Flag_AboveKey — RUONIA выше ключевой",
                marker=dict(color="orange", size=9, symbol="star"),
            ))
    fig1b.add_hline(y=0, line_color="gray", line_dash="dot", line_width=0.8)
    fig1b.add_hrect(y0=2, y1=11, fillcolor="red", opacity=0.04)
    fig1b.update_layout(
        title="М1: MAD-сигналы",
        yaxis=dict(title="Отклонение от нормы (σ)", range=[-5, 11]),
        height=300, legend=dict(x=0, y=1.15, orientation="h"),
    )
    st.plotly_chart(fig1b, use_container_width=True)
    st.caption("0 = норма · +3σ = стресс")
