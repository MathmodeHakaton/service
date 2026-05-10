"""
Страница 1: Обзор (LSI gauge + статус + вклад модулей)
"""

import streamlit as st
from datetime import datetime
from src.application.pipeline import Pipeline
from src.infrastructure.storage.db.engine import get_session


def show():
    """Показать страницу обзора"""

    st.header("📊 Обзор ликвидности")

    # Получить текущий LSI
    session = get_session()
    try:
        pipeline = Pipeline(session=session)
        lsi_result = pipeline.execute()

        # Метрики в три колонки
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("LSI Value", f"{lsi_result.value:.2%}")

        with col2:
            st.metric("Status", lsi_result.status.upper())

        with col3:
            st.metric("Timestamp", datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"))

        # Вклады модулей
        st.subheader("Вклады модулей")

        contrib_data = {
            "Модуль": list(lsi_result.contributions.keys()),
            "Вклад": [v for v in lsi_result.contributions.values()],
        }

        st.bar_chart(contrib_data)

        # Статус индикатор
        st.subheader("Статус ликвидности")

        if lsi_result.status == "critical":
            st.error("🔴 КРИТИЧЕСКИЙ уровень ликвидности")
        elif lsi_result.status == "warning":
            st.warning("🟡 ПРЕДУПРЕЖДЕНИЕ: риск ликвидности")
        else:
            st.success("🟢 НОРМАЛЬНЫЙ уровень ликвидности")
    finally:
        session.close()
