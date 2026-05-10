import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import streamlit as st
import plotly.graph_objects as go
import numpy as np

st.set_page_config(page_title="М5 · Казначейство", page_icon="📊", layout="wide")
st.title("М5 — Средства федерального казначейства")

from src.presentation.data_loader import load_all
with st.spinner("Загрузка..."):
    r1, r2, r3, r4, r5, lsi = load_all()

scores = lsi["scores"]
df5 = r5["signals_df"]

st.metric("Стресс-оценка М5", f"{scores.get('M5', 0):.1f} / 100")

if not df5.empty and "balance" in df5.columns:
    bal_now  = df5["balance"].iloc[-1]
    bal_sign = "дефицит" if bal_now > 0 else "профицит"
    st.caption(f"Источник: Банк России — структурный баланс (с 2019). Сейчас: {bal_sign} {abs(bal_now):.0f} млрд руб.")

    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(
        x=df5["date"], y=df5["balance"],
        fill="tozeroy",
        fillcolor="rgba(231,76,60,0.12)" if bal_now > 0 else "rgba(39,174,96,0.12)",
        line=dict(color="steelblue", width=1.8),
        name="Структурный баланс, млрд руб.",
    ))
    if "Flag_Budget_Drain" in df5.columns:
        drains = df5[df5["Flag_Budget_Drain"] == 1]
        if len(drains):
            fig5.add_trace(go.Scatter(
                x=drains["date"], y=drains["balance"], mode="markers",
                name="⚠ Flag_Budget_Drain (пик оттока > 500 млрд/нед)",
                marker=dict(color="red", size=8, symbol="triangle-down"),
            ))
    fig5.add_hline(y=0, line_color="black", line_width=1,
                   annotation_text="граница профицит/дефицит")
    fig5.update_layout(
        title="М5: Структурный баланс ликвидности банков",
        yaxis_title="Баланс, млрд руб.",
        height=340, legend=dict(x=0, y=1.18, orientation="h"),
    )
    st.plotly_chart(fig5, use_container_width=True)
    st.caption("Ниже нуля = профицит (норма). Выше нуля = дефицит (стресс).")

    fig5b = go.Figure()
    if "MAD_score_ЦБ" in df5.columns:
        fig5b.add_trace(go.Scatter(
            x=df5["date"], y=df5["MAD_score_ЦБ"],
            name="MAD_score_ЦБ — уровень баланса",
            line=dict(color="steelblue", width=1.8),
        ))
    if "MAD_score_Росказна" in df5.columns:
        fig5b.add_trace(go.Scatter(
            x=df5["date"], y=df5["MAD_score_Росказна"],
            name="MAD_score_Росказна — оттоки казначейства",
            line=dict(color="gray", width=1.2, dash="dot"),
        ))
    fig5b.add_hline(y=0, line_color="gray", line_dash="dot",
                    annotation_text="историческая норма", annotation_position="right")
    fig5b.update_layout(
        title="М5: MAD-сигналы",
        yaxis_title="Отклонение от нормы (σ)",
        height=260, legend=dict(x=0, y=1.18, orientation="h"),
    )
    st.plotly_chart(fig5b, use_container_width=True)
    st.caption("0 = норма · >0 дефицит (стресс) · <0 профицит (норма)")
