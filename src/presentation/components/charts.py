"""
Компоненты графиков для визуализации
"""

import streamlit as st
import plotly.graph_objects as go


def render_spread_chart(data):
    """Отрендерить график спредов"""

    fig = go.Figure()
    fig.add_trace(go.Scatter(y=data, mode='lines', name='Спред'))
    fig.update_layout(title="Спред актива",
                      xaxis_title="Время", yaxis_title="bp")

    st.plotly_chart(fig, use_container_width=True)


def render_cover_chart(data):
    """Отрендерить график покрытия"""

    fig = go.Figure()
    fig.add_trace(go.Scatter(y=data, mode='lines', name='Покрытие'))
    fig.update_layout(title="Покрытие приказов",
                      xaxis_title="Время", yaxis_title="Об./мин.")

    st.plotly_chart(fig, use_container_width=True)
