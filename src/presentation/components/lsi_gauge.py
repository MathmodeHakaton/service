"""
Компонент: LSI Gauge (визуализация индекса)
"""

import streamlit as st


def render_lsi_gauge(result):
    """Отрендерить LSI как датчик"""

    value = result.value

    if value >= 0.7:
        color = "🔴 Красная зона"
    elif value >= 0.4:
        color = "🟡 Жёлтая зона"
    else:
        color = "🟢 Зелёная зона"

    st.metric("Индекс ликвидности", f"{value:.2%}", delta=color)
