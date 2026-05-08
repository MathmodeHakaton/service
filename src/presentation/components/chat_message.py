"""
Компонент: Сообщение чата
"""

import streamlit as st


def render_chat_message(role: str, text: str):
    """Отрендерить сообщение чата"""

    with st.chat_message(role):
        st.write(text)
