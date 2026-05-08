"""
Страница 4: Аналитик (LLM чат - бонус)
"""

import streamlit as st
from src.domain.llm.local_model import LocalLLM
from src.domain.llm.prompt_builder import PromptBuilder
from src.infrastructure.storage.repository import Repository
import uuid


def show():
    """Показать страницу аналитика"""

    st.header("💬 AI Аналитик")

    # Инициализировать LLM
    llm = LocalLLM()

    if not llm.is_available():
        st.error("⚠️ LLM недоступна. Проверьте Ollama.")
        return

    # Инициализировать session state
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # История сообщений
    st.subheader("История чата")

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    # Input для пользователя
    if prompt := st.chat_input("Задайте вопрос аналитику..."):
        # Добавить сообщение пользователя
        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("user"):
            st.write(prompt)

        # Получить ответ от LLM
        with st.chat_message("assistant"):
            message_placeholder = st.empty()

            # Построить промпт
            full_prompt = PromptBuilder.build_chat_context_prompt(
                lsi_result=None,  # TODO: получить текущий LSI
                chat_history=st.session_state.messages
            )

            # Генерировать ответ
            response = llm.generate(full_prompt)
            message_placeholder.markdown(response)

            # Сохранить ответ
            st.session_state.messages.append(
                {"role": "assistant", "content": response})

            # TODO: сохранить в БД
