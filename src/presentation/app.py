"""
RU Liquidity Sentinel — Streamlit Dashboard (главная страница)
Запуск: streamlit run src/presentation/app.py --server.port 8501

На этой странице:
- Индекс стресса ликвидности (LSI) с гейджем
- Вклад модулей (M1-M5) в LSI
- Кнопки навигации на детальные страницы модулей (М1-М5, LSI, LLM)
"""
import logging
import os
import json
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

st.set_page_config(
    page_title="RU Liquidity Sentinel",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

@st.cache_data(ttl=1800)
def load_all():
    from src.application.pipeline import Pipeline
    from src.domain.aggregation.lsi_engine import LSIEngine
    from src.infrastructure.storage.db.engine import get_session
    from datetime import datetime

    session = get_session()
    try:
        p = Pipeline(session=session)
        result = p.execute_full()
        data = result.raw_data
        lsi_result = result.lsi

        engine = LSIEngine()

        lsi = {
            "lsi":           round(lsi_result.value * 100, 1),
            "status":        {"normal": "GREEN", "warning": "YELLOW",
                              "critical": "RED"}.get(lsi_result.status, "GREEN"),
            "contributions": {k: round(v * 100, 2) for k, v in lsi_result.contributions.items()},
            "scores":        {
                "M1": round(result.lsi.contributions.get("M1_RESERVES", 0) * 100, 1),
                "M2": round(result.lsi.contributions.get("M2_REPO", 0) * 100, 1),
                "M3": round(result.lsi.contributions.get("M3_OFZ", 0) * 100, 1),
                "M4": round(result.lsi.contributions.get("M4_TAX", 0) * 100, 1),
                "M5": round(result.lsi.contributions.get("M5_TREASURY", 0) * 100, 1),
            },
            "seasonal_factor": float(data.get("m4_multiplier", 1.0) or 1.0),
            "computed_at":   pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
        }

        return lsi
    finally:
        session.close()


def status_color(status):
    return {"GREEN": "#27ae60", "YELLOW": "#f39c12", "RED": "#e74c3c"}.get(status, "#95a5a6")


def status_ru(status):
    return {"GREEN": "НОРМА", "YELLOW": "ВНИМАНИЕ", "RED": "СТРЕСС"}.get(status, status)


def gauge_fig(value):
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title={"text": "Индекс стресса ликвидности", "font": {"size": 14}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1,
                     "tickvals": [0, 20, 40, 70, 100],
                     "ticktext": ["0", "20", "40 — внимание", "70 — стресс", "100"]},
            "bar": {"color": "#2c3e50"},
            "steps": [
                {"range": [0,  40], "color": "#d5f5e3"},
                {"range": [40, 70], "color": "#fdebd0"},
                {"range": [70, 100], "color": "#fadbd8"},
            ],
            "threshold": {"line": {"color": "black", "width": 3}, "value": value},
        },
        number={"suffix": " / 100", "font": {"size": 28}},
    ))
    fig.update_layout(height=230, margin=dict(t=40, b=10, l=10, r=10))
    return fig


with st.spinner("Загрузка данных..."):
    try:
        lsi = load_all()
        load_ok = True
    except Exception as e:
        st.error(f"Ошибка загрузки: {e}")
        import traceback
        st.code(traceback.format_exc())
        load_ok = False

if not load_ok:
    st.stop()

st.title("🇷🇺 RU Liquidity Sentinel")
st.caption(f"Обновлено: {lsi['computed_at']}  |  Источники: ЦБ РФ · Минфин · ФНС")

col_lsi, col_sf, col_status = st.columns([2, 1, 1])

with col_lsi:
    st.plotly_chart(gauge_fig(lsi['lsi']), use_container_width=True)

with col_sf:
    sf = lsi["seasonal_factor"]
    sf_labels = {1.4: "Конец квартала", 1.2: "Конец месяца", 1.1: "Налоговая неделя"}
    sf_label  = next((v for k, v in sf_labels.items() if sf >= k), "Норма")
    st.metric("Сезонный коэффициент", f"×{sf:.2f}")
    if sf >= 1.4:
        st.error(f"⚠ {sf_label}")
    elif sf >= 1.2:
        st.warning(f"↑ {sf_label}")
    elif sf >= 1.1:
        st.info(f"↑ {sf_label}")
    else:
        st.success("✓ Норма")

with col_status:
    color = status_color(lsi["status"])
    icon  = "🟢" if lsi["status"] == "GREEN" else "🟡" if lsi["status"] == "YELLOW" else "🔴"
    st.markdown(f"""
    <div style="background:{color};color:white;padding:24px 16px;
                border-radius:12px;text-align:center;margin-top:10px;">
        <div style="font-size:22px;font-weight:bold;">{icon} {status_ru(lsi['status'])}</div>
        <div style="font-size:36px;font-weight:bold;margin:6px 0;">{lsi['lsi']:.1f}</div>
        <div style="font-size:12px;opacity:0.85;">из 100 возможных</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

st.subheader("📊 Вклад модулей в индекс стресса")

MODULE_DESC = {
    "M1": "Корсчета + RUONIA",
    "M2": "Аукционы репо ЦБ",
    "M3": "Размещение ОФЗ",
    "M4": "Налоговый период",
    "M5": "Казначейство",
}

CONTRIB_KEY = {
    "M1": "M1_RESERVES", "M2": "M2_REPO",
    "M3": "M3_OFZ", "M4": "M4_TAX", "M5": "M5_TREASURY",
}

scores = lsi["scores"]

cols = st.columns(5)
for col, mk in zip(cols, ["M1", "M2", "M3", "M4", "M5"]):
    with col:
        sv = scores.get(mk, 0) or 0
        cv = lsi["contributions"].get(CONTRIB_KEY[mk], 0) or 0
        color = "#27ae60" if sv < 40 else "#f39c12" if sv < 70 else "#e74c3c"
        label = "норма" if sv < 40 else "внимание" if sv < 70 else "стресс"
        st.markdown(f"""
        <div style="border:2px solid {color};border-radius:10px;padding:12px;text-align:center;">
            <div style="font-size:9px;color:#666;margin-bottom:4px;">{MODULE_DESC[mk]}</div>
            <div style="font-size:26px;font-weight:bold;color:{color};">{sv:.0f}</div>
            <div style="font-size:9px;color:{color};font-weight:600;">{label}</div>
            <div style="font-size:9px;color:#999;">вклад: {cv:.1f}п</div>
        </div>
        """, unsafe_allow_html=True)

contrib_vals = [lsi["contributions"].get(CONTRIB_KEY[mk], 0) or 0 for mk in ["M1", "M2", "M3", "M4", "M5"]]
fig_bar = go.Figure(go.Bar(
    x=["M1", "M2", "M3", "M4", "M5"],
    y=contrib_vals,
    marker_color=[
        "#27ae60" if (scores.get(k, 0) or 0) < 40 else "#f39c12" if (scores.get(k, 0) or 0) < 70 else "#e74c3c"
        for k in ["M1", "M2", "M3", "M4", "M5"]
    ],
    text=[f"{v:.1f}" for v in contrib_vals],
    textposition="outside",
))
fig_bar.update_layout(height=240, margin=dict(t=10, b=10, l=10, r=10),
                      yaxis_title="Вклад (пт)", showlegend=False)
st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

st.subheader("🔗 Анализ модулей")

nav_cols = st.columns(6)
with nav_cols[0]:
    if st.button("📈 М1 · Резервы", use_container_width=True):
        st.switch_page("pages/1_М1.py")
with nav_cols[1]:
    if st.button("💰 М2 · Репо", use_container_width=True):
        st.switch_page("pages/2_М2.py")
with nav_cols[2]:
    if st.button("📋 М3 · ОФЗ", use_container_width=True):
        st.switch_page("pages/3_М3.py")
with nav_cols[3]:
    if st.button("📅 М4 · Налоги", use_container_width=True):
        st.switch_page("pages/4_М4.py")
with nav_cols[4]:
    if st.button("🏦 М5 · Казначейство", use_container_width=True):
        st.switch_page("pages/5_М5.py")
with nav_cols[5]:
    if st.button("🤖 Спросить LLM", use_container_width=True, type="secondary"):
        st.switch_page("pages/7_LLM.py")

st.divider()
st.caption("💡 Выберите модуль для детального анализа или задайте вопрос AI аналитику")