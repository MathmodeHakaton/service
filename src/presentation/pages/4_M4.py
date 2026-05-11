from src.presentation.data_loader import load_all
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


st.set_page_config(page_title="М4 · Налоги", page_icon="📊", layout="wide")
st.title("М4 — Налоговый период и сезонность")

with st.spinner("Загрузка..."):
    r1, r2, r3, r4, r5, lsi = load_all()

df4 = r4["signals_df"]
tax_df = r4.get("tax_df", pd.DataFrame())
m4_row = r4.get("latest", {})

st.metric("Seasonal_Factor сегодня",
          f"×{m4_row.get('Seasonal_Factor', 1.0):.2f}")
st.caption("Источник: Налоговый кодекс РФ — даты ключевых платежей (2014–2027)")

if tax_df is not None and not tax_df.empty:
    today_ts = pd.Timestamp.today()
    upcoming = tax_df[pd.to_datetime(
        tax_df["date"]) >= today_ts].head(8).copy()
    upcoming["date"] = pd.to_datetime(upcoming["date"]).dt.strftime("%d.%m.%Y")
    st.markdown("**Ближайшие налоговые даты:**")
    st.dataframe(upcoming[["date", "tax_type"]].rename(
        columns={"date": "Дата", "tax_type": "Налог"}
    ).reset_index(drop=True), width='stretch', hide_index=True)

if df4 is not None and not df4.empty:
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(
        x=df4["date"], y=df4["Seasonal_Factor"],
        fill="tozeroy", fillcolor="rgba(128,0,128,0.07)",
        line=dict(color="purple", width=1.5),
    ))
    for val, label in [(1.1, "Tax_Week"), (1.2, "End_of_Month"), (1.4, "End_of_Quarter")]:
        fig4.add_hline(y=val, line_color="purple", line_dash="dash", line_width=0.6,
                       annotation_text=label, annotation_position="right",
                       annotation_font_size=10)
    fig4.add_hline(y=1.0, line_color="gray", line_dash="dot")
    fig4.update_layout(
        title="М4: Seasonal_Factor — налоговый мультипликатор LSI",
        yaxis=dict(title="SF", range=[0.95, 1.5]), height=300, showlegend=False,
    )
    st.plotly_chart(fig4, width='stretch')
    st.caption(
        "×1.1 — налоговая неделя · ×1.2 — конец месяца · ×1.4 — конец квартала")
