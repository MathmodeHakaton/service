"""
Страница 2: Детализация модулей
"""

import streamlit as st
from src.application.pipeline import Pipeline
from src.infrastructure.storage.db.engine import get_session


def show():
    """Показать страницу модулей"""

    st.header("🔍 Детализация модулей")

    session = get_session()
    try:
        pipeline = Pipeline(session=session)

        # Получить сигналы от всех модулей
        data = pipeline._fetch_all()
        signals_df = pipeline._compute_signals(data)

        # Показать карточку для каждого модуля
        for module_name, signal_df in signals_df.items():
            if signal_df.empty:
                continue

            with st.expander(f"**{module_name}**"):
                # Показать последнее значение
                if not signal_df.empty:
                    last_row = signal_df.iloc[-1]
                    col1, col2 = st.columns(2)

                    with col1:
                        st.metric("Дата", last_row.get("date", "N/A"))

                    with col2:
                        st.metric("Строк", len(signal_df))

                # Тренд
                st.line_chart(signal_df.set_index("date")
                              if "date" in signal_df.columns else signal_df)
    finally:
        session.close()
