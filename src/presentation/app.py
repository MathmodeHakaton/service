"""
Главное Streamlit приложение
"""

import streamlit as st
from config.settings import get_settings

# Конфиг страницы
st.set_page_config(
    page_title="RU Liquidity Sentinel",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Стиль
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)


def main():
    """Главная функция приложения"""

    st.title("🌍 RU Liquidity Sentinel")
    st.markdown("Система мониторинга ликвидности рубля")

    # Боковая панель
    st.sidebar.title("Навигация")
    page = st.sidebar.radio(
        "Выбрать страницу",
        [
            "📊 Обзор",
            "🔍 Модули",
            "📈 Backtesting",
            "💬 Аналитик",
        ]
    )

    # Загрузить нужную страницу
    if page == "📊 Обзор":
        from src.presentation.pages.overview import show
        show()
    elif page == "🔍 Модули":
        from src.presentation.pages.modules import show
        show()
    elif page == "📈 Backtesting":
        from src.presentation.pages.backtest import show
        show()
    elif page == "💬 Аналитик":
        from src.presentation.pages.analyst import show
        show()


if __name__ == "__main__":
    main()
