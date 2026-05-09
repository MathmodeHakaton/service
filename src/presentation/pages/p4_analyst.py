"""Страница 4: AI Аналитик (LLM чат)."""

import streamlit as st


def show(result):
    st.title("💬 AI Аналитик")

    lsi     = result.lsi
    signals = {s.module_name: s for s in result.signals}

    # ── Контекст текущего LSI для промпта ─────────────────────────────────
    lsi_pct   = lsi.value * 100
    status_ru = {"normal":"Норма","warning":"Внимание","critical":"Стресс"}.get(lsi.status, lsi.status)
    context   = (
        f"Текущий LSI: {lsi_pct:.1f}/100 ({status_ru})\n"
        f"Вклад модулей: " +
        ", ".join(f"{k}={v*100:.1f}" for k, v in lsi.contributions.items()) + "\n"
        f"Активные флаги: " +
        ", ".join(f"{s.module_name}={s.latest_flag}"
                  for s in result.signals if s.latest_flag != "normal")
    )

    # ── LLM статус ────────────────────────────────────────────────────────
    try:
        from src.domain.llm.local_model import LocalLLM
        llm = LocalLLM()
        llm_ok = llm.is_available()
    except Exception:
        llm_ok = False

    if not llm_ok:
        st.warning(
            "⚠ LLM (Ollama) недоступна. Запустите: `docker-compose up -d` "
            "и убедитесь что Ollama слушает на localhost:11434"
        )
        st.info("Контекст для аналитика (скопируйте в любой LLM):")
        st.code(context)
        return

    # ── Чат интерфейс ─────────────────────────────────────────────────────
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    if prompt := st.chat_input("Задайте вопрос аналитику..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

        with st.chat_message("assistant"):
            from src.domain.llm.prompt_builder import PromptBuilder
            full_prompt = PromptBuilder.build_chat_context_prompt(
                lsi_result=lsi,
                chat_history=st.session_state.messages,
            )
            response = llm.generate(full_prompt)
            st.markdown(response)
            st.session_state.messages.append({"role": "assistant", "content": response})
