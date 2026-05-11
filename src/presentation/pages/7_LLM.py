"""
LLM Аналитик — RAG-чат по данным системы (YandexGPT через openai-клиент).

Архитектура:
    1) build_knowledge_base — собирает чанки из data/model_artifacts/* + статика ТЗ.
    2) retrieve — гибридный скоринг (token-overlap + IDF + теги).
    3) chat_llm.call_chat — диалог через openai-клиент Yandex AI Studio.

История диалога — в st.session_state["chat_history"] (формат OpenAI: role/content).
Off-topic-вопросы блокируются строгим system-промптом.
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pandas as pd
import streamlit as st

st.set_page_config(page_title="LLM · Аналитик", page_icon="💬", layout="wide")

from config.settings import get_settings
from src.presentation.rag.knowledge_base import build_knowledge_base
from src.presentation.rag.retriever import retrieve
from src.presentation.rag.chat_llm import call_chat

settings = get_settings()
creds_ok = bool(settings.yandex_api_key and settings.yandex_folder_id)

st.title("LLM — AI Аналитик")
st.caption("RAG-чат по артефактам модели и описаниям модулей. "
           "Отвечает строго по документам системы.")


# ── KB ────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner=False)
def _tax_calendar():
    try:
        from src.application.pipeline import Pipeline
        from src.infrastructure.storage.db.engine import get_session
        s = get_session()
        try:
            return Pipeline(session=s).execute_full().raw_data.get(
                "tax_calendar", pd.DataFrame())
        finally:
            s.close()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def _kb():
    return build_knowledge_base(tax_df=_tax_calendar())


chunks = _kb()


# ── Сайдбар ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.subheader("Параметры")
    top_k = st.slider("Чанков в контексте (top-k)", 3, 12, 6)
    if st.button("🧹 Очистить историю"):
        st.session_state.pop("chat_history", None)
        st.rerun()
    if st.button("🔄 Пересобрать KB"):
        _kb.clear()
        st.rerun()
    st.divider()
    st.write("**Yandex AI Studio:** " + ("✅ ключи заданы" if creds_ok else "⛔ нет ключей"))
    st.caption(f"Модель: `{settings.yandex_model_chat}`")
    st.caption(f"Чанков в KB: **{len(chunks)}**")


with st.expander("📚 Все чанки knowledge base"):
    for c in chunks:
        st.markdown(f"**{c.title}** *({c.kind}, tags: {', '.join(sorted(c.tags)) or '—'})*")
        st.write(c.text)


# ── История диалога (формат OpenAI: role/content) ─────────────────────────
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

for msg in st.session_state.chat_history:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg["role"] == "assistant" and msg.get("ctx_titles"):
            with st.expander("🔍 RAG-контекст этого ответа"):
                for t in msg["ctx_titles"]:
                    st.write(f"• {t}")


# ── Ввод ──────────────────────────────────────────────────────────────────
prompt = st.chat_input("Спросите про LSI, периоды или модули. "
                       "Пример: «что происходило с ликвидностью в марте 2022?»")
if prompt:
    if not creds_ok:
        st.error("Не заданы YANDEX_API_KEY и YANDEX_FOLDER_ID — чат отключён. "
                 "Добавьте их в `.env` или переменные окружения.")
    else:
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        retrieved = retrieve(prompt, chunks, k=top_k)

        with st.chat_message("assistant"):
            placeholder = st.empty()
            placeholder.markdown("_думаю…_")
            try:
                answer = call_chat(
                    messages=st.session_state.chat_history,
                    chunks=retrieved,
                    temperature=0.0,
                )
            except Exception as e:
                answer = f"⚠ Ошибка Yandex API: {e}"
            placeholder.markdown(answer)
            with st.expander("🔍 RAG-контекст этого ответа"):
                for c in retrieved:
                    st.write(f"• {c.title}")

        st.session_state.chat_history.append({
            "role": "assistant",
            "content": answer,
            "ctx_titles": [c.title for c in retrieved],
            "ts": datetime.now().isoformat(timespec="seconds"),
        })
