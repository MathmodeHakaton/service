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
from src.presentation.rag.query_rewrite import needs_rewrite, rewrite_query
import time
from src.presentation.rag.chat_llm import call_chat
from src.presentation.rag.guardrails import (
    CANONICAL_REFUSAL, is_prompt_injection, looks_like_refusal,
    filter_history_for_llm,
)
from src.presentation.rag.chat_llm import call_chat, build_system_prompt
from src.presentation.rag.retriever import retrieve
from src.presentation.rag.knowledge_base import build_knowledge_base
from config.settings import get_settings
import streamlit as st
import pandas as pd

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


st.set_page_config(page_title="LLM · Аналитик", page_icon="💬", layout="wide")


# Сколько последних реплик отдаём в LLM. Защита от раздувания контекста.
HISTORY_TURNS_FOR_LLM = 6

settings = get_settings()
creds_ok = bool(settings.yandex_api_key and settings.yandex_folder_id)

st.title("LLM — AI Аналитик")
st.caption("RAG-чат по артефактам модели и описаниям модулей. "
           "Отвечает строго по документам системы.")

# ── Анимация загрузки (только в начале) ────────────────────────────────────
if "loading_shown" not in st.session_state:
    st.session_state.loading_shown = False

if not st.session_state.loading_shown:
    placeholder_loading = st.empty()
    with placeholder_loading.container():
        col1, col2, col3 = st.columns([1, 2, 1], vertical_alignment="center")
        with col2:
            st.markdown("""
            <div style="text-align: center; padding: 30px;">
                <div style="font-size: 48px; margin-bottom: 20px; animation: spin 2s linear infinite;">⚡</div>
                <h3>Инициализация LLM Аналитика...</h3>
                <div style="margin-top: 20px;">
                    <div style="
                        width: 100%;
                        height: 8px;
                        background: #e0e0e0;
                        border-radius: 4px;
                        overflow: hidden;
                    ">
                        <div style="
                            width: 100%;
                            height: 100%;
                            background: linear-gradient(90deg, #4CAF50, #45a049);
                            animation: progress 3s ease-in-out forwards;
                        "></div>
                    </div>
                </div>
                <p style="margin-top: 20px; color: #666;">Загружаю модель и knowledge base...</p>
            </div>
            <style>
                @keyframes progress {
                    0% { width: 0%; }
                    50% { width: 70%; }
                    100% { width: 100%; }
                }
                @keyframes spin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                }
            </style>
            """, unsafe_allow_html=True)
        time.sleep(3)

    placeholder_loading.empty()
    st.session_state.loading_shown = True


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
    st.write("**Yandex AI Studio:** " +
             ("✅ ключи заданы" if creds_ok else "⛔ нет ключей"))
    st.caption(f"Модель: `{settings.yandex_model_chat}`")
    st.caption(f"Чанков в KB: **{len(chunks)}**")


with st.expander("📚 Все чанки knowledge base"):
    for c in chunks:
        st.markdown(
            f"**{c.title}** *({c.kind}, tags: {', '.join(sorted(c.tags)) or '—'})*")
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
    elif is_prompt_injection(prompt):
        # Инъекции/jailbreak не попадают в историю — ни запрос, ни отказ.
        # Канонический ответ показываем разово, контекст диалога остаётся чистым.
        with st.chat_message("user"):
            st.markdown(prompt)
        with st.chat_message("assistant"):
            st.markdown(CANONICAL_REFUSAL)
            st.caption("🛡 Заблокировано локальным guardrail (prompt-injection). "
                       "В историю не сохранено.")
    else:
        st.session_state.chat_history.append(
            {"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # 1) Query rewrite: если запрос короткий/ссылочный — переписываем
        # через LLM, опираясь на предыдущие реплики пользователя.
        prev_user_turns = [m["content"] for m in st.session_state.chat_history[:-1]
                           if m.get("role") == "user"]
        if needs_rewrite(prompt) and prev_user_turns:
            search_query = rewrite_query(prompt, prev_user_turns)
        else:
            search_query = prompt

        # 2) Retrieve по переписанному запросу, плюс «контекст диалога» (предыдущая
        # пользовательская реплика) с пониженным весом — для бустинга сущностей.
        prev_q = prev_user_turns[-1] if prev_user_turns else None
        retrieved = retrieve(search_query, chunks,
                             k=top_k, prev_user_query=prev_q)

        # 3) История для LLM — отфильтрованная от refusal-пар + ограниченная по длине.
        clean_history = filter_history_for_llm(st.session_state.chat_history)
        if len(clean_history) > HISTORY_TURNS_FOR_LLM:
            clean_history = clean_history[-HISTORY_TURNS_FOR_LLM:]

        sent_system_prompt = build_system_prompt(retrieved)

        with st.chat_message("assistant"):
            placeholder = st.empty()
            placeholder.markdown("_думаю…_")
            try:
                answer = call_chat(
                    messages=clean_history,
                    chunks=retrieved,
                    temperature=0.0,
                )
            except Exception as e:
                answer = f"⚠ Ошибка Yandex API: {e}"
                refused = False
            else:
                refused = looks_like_refusal(answer)
            placeholder.markdown(answer)

            with st.expander("🔍 RAG-контекст этого ответа"):
                for c in retrieved:
                    st.write(f"• {c.title}")

            with st.expander("🩺 Debug: что реально ушло в LLM"):
                if search_query != prompt:
                    st.markdown(
                        f"**Query rewrite:** `{prompt}` → `{search_query}`")
                st.markdown(
                    f"**Извлечено чанков:** {len(retrieved)} / запрошено {top_k}")
                st.markdown(f"**Помечен как refused:** `{refused}`")
                st.markdown("**Чанки (title · kind · tags · превью):**")
                for i, c in enumerate(retrieved, 1):
                    tags = ", ".join(sorted(c.tags)) or "—"
                    preview = (c.text or "").strip().replace("\n", " ")
                    if len(preview) > 240:
                        preview = preview[:240] + "…"
                    st.write(
                        f"{i}. **{c.title}**  ·  *{c.kind}*  ·  tags: `{tags}`")
                    st.caption(preview)
                st.markdown("**Filtered history (отправлено в LLM):**")
                st.json(clean_history)
                st.markdown("**System prompt (с подставленным контекстом):**")
                st.code(sent_system_prompt, language="markdown")

        st.session_state.chat_history.append({
            "role": "assistant",
            "content": answer,
            "ctx_titles": [c.title for c in retrieved],
            "refused": refused,
            "ts": datetime.now().isoformat(timespec="seconds"),
        })
