from __future__ import annotations
from config.settings import get_settings
import streamlit as st
import sys
from pathlib import Path
from datetime import timedelta
import pandas as pd
import plotly.graph_objects as go
"""
LSI · Liquidity Stress Index — финальная страница агрегационного слоя.

Контракт:
    • Артефакты модели — `data/model_artifacts/`.
        Туда `src.application.lsi_refresh.refresh_lsi()` кладёт CSV из ml_model.
    • Кнопка «🔄 Обновить» (или планировщик) запускает полный цикл:
        парсеры → upsert ml_model/data → run_pipeline.py → copy outputs.
    • Авто-комментарий — Yandex AI Studio через `ml_model.src.llm_commentator`.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


st.set_page_config(page_title="LSI · Агрегация", page_icon="📊", layout="wide")

ROOT = Path(__file__).resolve().parents[3]
ART = ROOT / "data" / "model_artifacts"
MODULE_LABELS = {"M1": "М1 · Резервы", "M2": "М2 · Репо ЦБ", "M3": "М3 · ОФЗ",
                 "M4": "М4 · Сезонность", "M5": "М5 · Казначейство"}
STATUS_COLOR = {"green": "#27ae60", "yellow": "#f39c12", "red": "#e74c3c",
                "partial": "#95a5a6"}
STATUS_RU = {"green": "🟢 НОРМА", "yellow": "🟡 ВНИМАНИЕ", "red": "🔴 СТРЕСС",
             "partial": "⚪ ЧАСТИЧНО"}


# ДАННЫЕ

@st.cache_data(ttl=600)
def load_artifacts():
    extract = pd.read_csv(
        ART / "lsi_dashboard_extract.csv", parse_dates=["date"])
    mod_imp = pd.read_csv(ART / "module_importance_catboost.csv")
    feat_imp = pd.read_csv(ART / "feature_importance.csv")
    backtest = pd.read_csv(ART / "backtest_crisis_episodes.csv")
    full_ts = pd.read_csv(ART / "lsi_timeseries.csv", parse_dates=["date"])
    return extract, mod_imp, feat_imp, backtest, full_ts


@st.cache_data(ttl=600)
def artifacts_mtime() -> str:
    p = ART / "lsi_dashboard_extract.csv"
    if not p.exists():
        return "—"
    return pd.Timestamp(p.stat().st_mtime, unit="s",
                        tz="Europe/Moscow").strftime("%Y-%m-%d %H:%M")


try:
    extract, mod_imp, feat_imp, backtest, full_ts = load_artifacts()
except FileNotFoundError as e:
    st.error(f"Не найдены артефакты модели: {e}. Ожидаются в {ART}.")
    st.info("Запустите обновление: кнопка «🔄 Обновить LSI» или "
            "`python -c \"from src.application.lsi_refresh import refresh_lsi; "
            "print(refresh_lsi())\"`.")
    st.stop()


# ШАПКА + REFRESH

c_h, c_btn1, c_btn2 = st.columns([4, 1, 1])
with c_h:
    st.title("LSI — Liquidity Stress Index")
    st.caption(f"Артефакты обновлены: **{artifacts_mtime()}** · "
               f"источник: CatBoost + SHAP, "
               f"`data/model_artifacts/lsi_dashboard_extract.csv`")


def _do_refresh(mode: str, spinner_text: str):
    from src.application.lsi_refresh import refresh_lsi
    with st.spinner(spinner_text):
        rep = refresh_lsi(mode=mode)
    secs = (pd.Timestamp(rep.finished_at) -
            pd.Timestamp(rep.started_at)).total_seconds()
    if rep.ok:
        st.success(f"{rep.mode.capitalize()} готов за {secs:.0f} с. "
                   f"Артефактов скопировано: {rep.artifacts_copied}.")
    else:
        st.error(f"Ошибка ({rep.mode}): {rep.error}")
    with st.expander("Лог обновления"):
        st.json({"mode": rep.mode, "upsert": rep.upsert,
                 "ml_log_tail": rep.ml_log_tail,
                 "started_at": rep.started_at,
                 "finished_at": rep.finished_at})
    st.cache_data.clear()
    st.rerun()


with c_btn1:
    st.write("")
    if st.button("🔄 Обновить LSI", type="primary",
                 help="Парсеры → upsert ml_model/data → inference.py "
                      "(predict + SHAP, БЕЗ переобучения). Обычно 5–20 секунд."):
        _do_refresh("inference", "Загружаю данные и инферю LSI…")

with c_btn2:
    st.write("")
    if st.button("🧠 Переобучить",
                 help="Полный retrain CatBoost на всей истории. "
                      "Делается еженедельно автоматом; вручную — если "
                      "появилась новая длинная история или изменились фичи. "
                      "Длительность 1–3 мин."):
        _do_refresh("retrain", "Переобучаю CatBoost (1–3 мин)…")


# Свежий день: последний валидный.
valid = extract[extract["full_model_valid"] == 1]
latest = (valid.iloc[-1] if not valid.empty else extract.iloc[-1])
latest_date = pd.to_datetime(latest["date"]).date()
contributions = {m: float(latest.get(f"contribution_{m}", 0.0) or 0.0)
                 for m in ["M1", "M2", "M3", "M4", "M5"]}
lsi_val = float(latest.get("lsi") if pd.notna(latest.get("lsi"))
                else latest["lsi_smoothed"])
lsi_raw = float(latest["lsi_raw"])
status = str(latest["status"])


# ГЕЙДЖ + СТАТУС
col_g, col_s = st.columns([2, 1])
with col_g:
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=lsi_val,
        title={"text": f"LSI на {latest_date.isoformat()}", "font": {
            "size": 14}},
        gauge={
            "axis": {"range": [0, 100],
                     "tickvals": [0, 40, 70, 100],
                     "ticktext": ["0", "40", "70", "100"]},
            "bar": {"color": "#2c3e50"},
            "steps": [
                {"range": [0, 40], "color": "#d5f5e3"},
                {"range": [40, 70], "color": "#fdebd0"},
                {"range": [70, 100], "color": "#fadbd8"},
            ],
            "threshold": {"line": {"color": "black", "width": 3}, "value": lsi_val},
        },
        number={"suffix": " / 100", "font": {"size": 32}},
    ))
    fig.update_layout(height=260, margin=dict(t=30, b=10, l=10, r=10))
    st.plotly_chart(fig, width='stretch')

with col_s:
    color = STATUS_COLOR.get(status, "#95a5a6")
    st.markdown(f"""
    <div style="background:{color};color:white;padding:32px 16px;
                border-radius:12px;text-align:center;margin-top:30px;">
        <div style="font-size:22px;font-weight:bold;">{STATUS_RU.get(status, status)}</div>
        <div style="font-size:48px;font-weight:bold;margin:8px 0;">{lsi_val:.1f}</div>
        <div style="font-size:12px;opacity:0.85;">LSI сглаженный (Калман+гистерезис)</div>
        <div style="font-size:11px;opacity:0.7;margin-top:6px;">сырой: {lsi_raw:.1f} · SF×{latest['m4_multiplier']:.2f}</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# ВКЛАД МОДУЛЕЙ (SHAP)
st.subheader("Вклад модулей в текущий LSI (SHAP)")
st.caption("SHAP-разложение прогноза CatBoost. "
           "Положительный вклад тянет вверх (к стрессу), отрицательный — вниз. "
           "M4 — мультипликативный эффект.")

contrib_df = pd.DataFrame({
    "module": [MODULE_LABELS[m] for m in ["M1", "M2", "M3", "M4", "M5"]],
    "contribution": [contributions[m] for m in ["M1", "M2", "M3", "M4", "M5"]],
}).sort_values("contribution", key=lambda s: s.abs(), ascending=False)
colors = ["#e74c3c" if v > 0 else "#27ae60" for v in contrib_df["contribution"]]
fig_c = go.Figure(go.Bar(
    x=contrib_df["contribution"], y=contrib_df["module"],
    orientation="h", marker_color=colors,
    text=[f"{v:+.2f}" for v in contrib_df["contribution"]], textposition="outside",
))
fig_c.add_vline(x=0, line_color="gray", line_dash="dot")
fig_c.update_layout(height=300, margin=dict(t=10, b=10, l=10, r=20),
                    xaxis_title="Вклад в LSI (пунктов)")
st.plotly_chart(fig_c, width='stretch')

# LSI TIMESERIES
st.subheader("История LSI")
ts = extract.dropna(subset=["lsi_smoothed"]).copy()
ts["status"] = ts["status"].astype(str)
fig_ts = go.Figure()
for st_key, color in STATUS_COLOR.items():
    sub = ts[ts["status"] == st_key]
    if not sub.empty:
        fig_ts.add_trace(go.Scatter(
            x=sub["date"], y=sub["lsi_smoothed"], mode="markers",
            name=STATUS_RU.get(st_key, st_key),
            marker=dict(color=color, size=3, opacity=0.55),
        ))
fig_ts.add_trace(go.Scatter(
    x=ts["date"], y=ts["lsi_smoothed"], mode="lines",
    line=dict(color="#2c3e50", width=1.2), showlegend=False,
))
for s, e, lbl in [("2022-02-01", "2022-04-30", "Фев-апр 2022"),
                  ("2023-08-01", "2023-09-30", "Авг-сен 2023")]:
    fig_ts.add_vrect(x0=s, x1=e, fillcolor="red", opacity=0.07,
                     annotation_text=lbl, annotation_position="top left",
                     annotation_font_size=10)
fig_ts.add_hline(y=40, line_dash="dash", line_color="#27ae60")
fig_ts.add_hline(y=70, line_dash="dash", line_color="#e74c3c")
fig_ts.update_layout(height=420, yaxis=dict(range=[0, 105], title="LSI"),
                     xaxis_title="Дата", legend=dict(x=0, y=1.12, orientation="h"))
st.plotly_chart(fig_ts, use_container_width=True)

with st.expander("🧪 Backtest на стресс-эпизодах ТЗ"):
    st.dataframe(backtest, use_container_width=True, hide_index=True)


# YANDEX LLM КОММЕНТАРИЙ
st.divider()
st.subheader("🤖 Комментарий аналитика (Yandex AI Studio)")

_s = get_settings()
api_key = _s.yandex_api_key
folder_id = _s.yandex_folder_id

if not api_key or not folder_id:
    st.warning("Не заданы `YANDEX_API_KEY` и `YANDEX_FOLDER_ID` "
               "(переменные окружения или `.env`). "
               "Комментарий не сгенерирован.")
    st.info(
        f"**Эвристический фолбэк:** LSI={lsi_val:.1f} ({STATUS_RU.get(status, status)}). "
        f"Главный драйвер: "
        f"{max(contributions.items(), key=lambda x: x[1])[0]} "
        f"({max(contributions.values()):+.2f} пт). "
        f"Сезонный мультипликатор: ×{latest['m4_multiplier']:.2f}."
    )
else:
    from src.presentation.rag.commentary_prompt import (
        SYSTEM_PROMPT as COMMENTARY_SYS, build_context, build_user_prompt,
    )
    from src.presentation.rag.yandex_client import complete as yc_complete

    @st.cache_data(ttl=900, show_spinner=False)
    def _commentary(ts_hash: str):
        ctx = build_context(full_ts)
        prompt = build_user_prompt(ctx)
        text = yc_complete(
            system_text=COMMENTARY_SYS,
            user_text=prompt,
            model=_s.yandex_model_commentary,
            temperature=0.0,
            max_tokens=700,
        )
        return text, ctx, prompt

    with st.spinner(f"YandexGPT ({_s.yandex_model_commentary}) генерирует…"):
        try:
            text, ctx, prompt = _commentary(artifacts_mtime())
        except Exception as e:
            st.error(f"YandexGPT не ответил: {e}")
            text = None

    if text:
        st.markdown(
            f"<div style='background:#f4f6f8;padding:18px;border-radius:10px;"
            f"border-left:4px solid #2c3e50;line-height:1.55;'>{text}</div>",
            unsafe_allow_html=True,
        )
        st.caption(f"Модель: `{_s.yandex_model_commentary}` · "
                   f"LSI={ctx['lsi']} · "
                   f"Δнед={ctx['delta_week']:+.1f} · "
                   f"Δмес={ctx['delta_month']:+.1f}")
        with st.expander("📝 Числовой контекст и промпт"):
            st.json(ctx)
            st.code(prompt, language="text")
