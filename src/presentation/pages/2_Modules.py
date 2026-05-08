"""
Страница 2: Детализация модулей
"""

import streamlit as st
from src.application.pipeline import Pipeline


def show():
    """Показать страницу модулей"""

    st.header("🔍 Детализация модулей")

    pipeline = Pipeline()

    # Получить сигналы от всех модулей
    data = pipeline._fetch_data()
    signals = pipeline._compute_module_signals(data)

    # Показать карточку для каждого модуля
    for signal in signals:
        with st.expander(f"**{signal.module_name}** - {signal.latest_flag.upper()}"):
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Значение", f"{signal.value:.2%}")

            with col2:
                st.metric("Флаг", signal.latest_flag)

            with col3:
                st.metric("Вклад", f"{signal.contribution:.2%}")

            # Тренд
            st.line_chart(signal.mad_scores)
