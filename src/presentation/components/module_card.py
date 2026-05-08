"""
Компонент: Карточка модуля
"""

import streamlit as st


def render_module_card(signal):
    """Отрендерить карточку модуля"""

    with st.container():
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(f"{signal.module_name}", f"{signal.value:.2%}")

        with col2:
            st.metric("Флаг", signal.latest_flag)

        with col3:
            st.metric("Вклад", f"{signal.contribution:.2%}")
