import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np

st.set_page_config(page_title="М3 · ОФЗ", page_icon="📊", layout="wide")
st.title("М3 — Размещение ОФЗ Минфина")

from src.presentation.data_loader import load_all
with st.spinner("Загрузка..."):
    r1, r2, r3, r4, r5, lsi = load_all()

scores = lsi["scores"]
df3 = r3["signals_df"]

st.metric("Стресс-оценка М3", f"{scores.get('M3', 0):.1f} / 100")
st.caption("Источник: Министерство финансов РФ — результаты аукционов ОФЗ (2026)")

if df3 is not None and not df3.empty and "cover_ratio" in df3.columns:
    auctions3 = df3.dropna(subset=["cover_ratio"]).copy().reset_index(drop=True)
    if len(auctions3):
        auctions3["rank"]      = auctions3.groupby("date").cumcount()
        auctions3["n_per_day"] = auctions3.groupby("date")["rank"].transform("count")
        auctions3["x_offset"]  = auctions3.apply(
            lambda r: pd.Timedelta(days=-0.3) if r["n_per_day"] > 1 and r["rank"] == 0
            else (pd.Timedelta(days=0.3) if r["n_per_day"] > 1 and r["rank"] == 1
            else pd.Timedelta(0)), axis=1
        )
        auctions3["x"] = auctions3["date"] + auctions3["x_offset"]
        colors3 = [
            "crimson"   if c < 1.2 else
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
            title="М3: Cover ratio аукционов ОФЗ (спрос / размещение)",
            yaxis_title="cover_ratio", xaxis=dict(type="date"), height=320,
        )
        st.plotly_chart(fig3, use_container_width=True)
        st.markdown("🔴 Недоспрос (< 1.2) &nbsp;&nbsp; 🔵 Переспрос (> 2.0) &nbsp;&nbsp; ⚪ Норма (1.2 – 2.0)")
        st.caption("Два столбика на дату = два выпуска ОФЗ в один день.")

    if "avg_yield" in df3.columns:
        fig3b = go.Figure()
        fig3b.add_trace(go.Scatter(
            x=df3["date"], y=df3["avg_yield"],
            line=dict(color="darkorange", width=2),
        ))
        fig3b.update_layout(
            title="М3: Доходность ОФЗ",
            yaxis_title="Доходность, % годовых", height=240, showlegend=False,
        )
        st.plotly_chart(fig3b, use_container_width=True)
else:
    st.info("Нет данных ОФЗ — нужны результаты аукционов Минфина")
