"""
Страница 3: Backtesting (исторические эпизоды)
"""

import streamlit as st
from src.application.backtest import BacktestRunner


def show():
    """Показать страницу backtesting"""

    st.header("📈 Backtesting")

    col1, col2 = st.columns(2)

    with col1:
        start_year = st.slider("Начальный год", 2014, 2023, 2014)

    with col2:
        end_year = st.slider("Конечный год", 2014, 2023, 2023)

    if st.button("Запустить backtesting"):
        runner = BacktestRunner(start_year=start_year, end_year=end_year)
        results = runner.run()

        values = [r.value for r in results]
        st.line_chart(values)

        metrics = runner.get_metrics(results)

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Sharpe Ratio", f"{metrics['sharpe_ratio']:.2f}")

        with col2:
            st.metric("Max Drawdown", f"{metrics['max_drawdown']:.2%}")

        with col3:
            st.metric("Total Return", f"{metrics['total_return']:.2%}")

        with col4:
            st.metric("Win Rate", f"{metrics['win_rate']:.2%}")
