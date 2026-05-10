import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

st.set_page_config(page_title="М2 · Репо ЦБ", page_icon="📊", layout="wide")
st.title("М2 — Аукционы репо ЦБ (7-дневные)")

from src.presentation.data_loader import load_all
with st.spinner("Загрузка..."):
    r1, r2, r3, r4, r5, lsi = load_all()

scores = lsi["scores"]
df2 = r2["signals_df"]

st.metric("Стресс-оценка М2", f"{scores.get('M2', 0):.1f} / 100")
st.caption("Источник: Банк России — итоги недельных аукционов репо (с 2010)")

if not df2.empty and "rate_spread" in df2.columns:
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=df2["date"], y=df2["rate_spread"],
        name="Переплата над ключевой ставкой, п.п.",
        line=dict(color="darkorange", width=1.5),
        fill="tozeroy", fillcolor="rgba(255,165,0,0.1)",
    ))
    if "MAD_score_rate_spread" in df2.columns:
        fig2.add_trace(go.Scatter(
            x=df2["date"], y=df2["MAD_score_rate_spread"],
            name="MAD_score_rate_spread (σ)",
            yaxis="y2", line=dict(color="crimson", width=1.5, dash="dot"),
        ))
    if "Flag_Demand" in df2.columns:
        flags2 = df2[df2["Flag_Demand"] == 1]
        if len(flags2):
            fig2.add_trace(go.Scatter(
                x=flags2["date"], y=flags2["rate_spread"], mode="markers",
                name="🔴 Flag_Demand — острый переспрос",
                marker=dict(color="red", size=9, symbol="triangle-up"),
            ))
    fig2.add_hline(y=0, line_color="gray", line_dash="dot", line_width=0.8)
    fig2.update_layout(
        title="М2: Переплата банков на аукционах репо ЦБ",
        yaxis=dict(title="Переплата, п.п."),
        yaxis2=dict(title="MAD (σ)", overlaying="y", side="right"),
        height=380, legend=dict(x=0, y=1.18, orientation="h"),
    )
    st.plotly_chart(fig2, use_container_width=True)
    st.caption("Переплата > 0 = банки готовы брать деньги дороже ключевой ставки.")
